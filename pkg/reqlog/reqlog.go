package reqlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/filter"
	"github.com/dstotijn/hetty/pkg/log"
	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/scope"
)

type contextKey int

const (
	LogBypassedKey contextKey = iota
	ReqLogIDKey
	projectIDKey
)

var (
	ErrRequestNotFound    = errors.New("reqlog: request not found")
	ErrProjectIDMustBeSet = errors.New("reqlog: project ID must be set")
)

type RequestLog struct {
	ID        ulid.ULID
	ProjectID ulid.ULID

	URL    *url.URL
	Method string
	Proto  string
	Header http.Header
	Body   []byte

	Response *ResponseLog
}

type ResponseLog struct {
	Proto      string
	StatusCode int
	Status     string
	Header     http.Header
	Body       []byte
}

type Service struct {
	mu                       sync.RWMutex
	bypassOutOfScopeRequests bool
	findReqsFilter           FindRequestsFilter
	activeProjectID          ulid.ULID
	scope                    *scope.Scope
	repo                     Repository
	logger                   log.Logger
}

type FindRequestsFilter struct {
	ProjectID   ulid.ULID
	OnlyInScope bool
	SearchExpr  filter.Expression
}

type Config struct {
	ActiveProjectID ulid.ULID
	Scope           *scope.Scope
	Repository      Repository
	Logger          log.Logger
}

func NewService(cfg Config) *Service {
	s := &Service{
		activeProjectID: cfg.ActiveProjectID,
		repo:            cfg.Repository,
		scope:           cfg.Scope,
		logger:          cfg.Logger,
	}

	if s.logger == nil {
		s.logger = log.NewNopLogger()
	}

	return s
}

func (svc *Service) FindRequests(ctx context.Context) ([]RequestLog, error) {
	svc.mu.RLock()
	filter := svc.findReqsFilter
	svc.mu.RUnlock()

	return svc.repo.FindRequestLogs(ctx, filter, svc.scope)
}

func (svc *Service) FindRequestLogByID(ctx context.Context, id ulid.ULID) (RequestLog, error) {
	return svc.repo.FindRequestLogByID(ctx, svc.ActiveProjectID(), id)
}

func (svc *Service) ClearRequests(ctx context.Context, projectID ulid.ULID) error {
	return svc.repo.ClearRequestLogs(ctx, projectID)
}

func (svc *Service) storeResponse(ctx context.Context, projectID, reqLogID ulid.ULID, res *http.Response) error {
	resLog, err := ParseHTTPResponse(res)
	if err != nil {
		return err
	}

	return svc.repo.StoreResponseLog(ctx, projectID, reqLogID, resLog)
}

func (svc *Service) RequestModifier(next proxy.RequestModifyFunc) proxy.RequestModifyFunc {
	return func(req *http.Request) {
		next(req)

		clone := req.Clone(req.Context())

		var body []byte

		if req.Body != nil {
			// TODO: Use io.LimitReader.
			var err error

			body, err = ioutil.ReadAll(req.Body)
			if err != nil {
				svc.logger.Errorw("Failed to read request body for logging.",
					"error", err)
				return
			}

			req.Body = ioutil.NopCloser(bytes.NewBuffer(body))
			clone.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		}

		svc.mu.RLock()
		projectID := svc.activeProjectID
		bypassOutOfScope := svc.bypassOutOfScopeRequests
		svc.mu.RUnlock()

		// Bypass logging if no project is active.
		if projectID.Compare(ulid.ULID{}) == 0 {
			ctx := context.WithValue(req.Context(), LogBypassedKey, true)
			*req = *req.WithContext(ctx)

			svc.logger.Debugw("Bypassed logging: no active project.",
				"url", req.URL.String())

			return
		}

		// Bypass logging if this setting is enabled and the incoming request
		// doesn't match any scope rules.
		if bypassOutOfScope && !svc.scope.Match(clone, body) {
			ctx := context.WithValue(req.Context(), LogBypassedKey, true)
			*req = *req.WithContext(ctx)

			svc.logger.Debugw("Bypassed logging: request doesn't match any scope rules.",
				"url", req.URL.String())

			return
		}

		reqID, ok := proxy.RequestIDFromContext(req.Context())
		if !ok {
			svc.logger.Errorw("Bypassed logging: request doesn't have an ID.")
			return
		}

		reqLog := RequestLog{
			ID:        reqID,
			ProjectID: projectID,
			Method:    clone.Method,
			URL:       clone.URL,
			Proto:     clone.Proto,
			Header:    clone.Header,
			Body:      body,
		}

		err := svc.repo.StoreRequestLog(req.Context(), reqLog)
		if err != nil {
			svc.logger.Errorw("Failed to store request log.",
				"error", err)
			return
		}

		svc.logger.Debugw("Stored request log.",
			"reqLogID", reqLog.ID.String(),
			"url", reqLog.URL.String())

		ctx := context.WithValue(req.Context(), ReqLogIDKey, reqLog.ID)
		ctx = context.WithValue(ctx, projectIDKey, projectID)
		*req = *req.WithContext(ctx)
	}
}

func (svc *Service) ResponseModifier(next proxy.ResponseModifyFunc) proxy.ResponseModifyFunc {
	return func(res *http.Response) error {
		if err := next(res); err != nil {
			return err
		}

		if bypassed, _ := res.Request.Context().Value(LogBypassedKey).(bool); bypassed {
			return nil
		}

		reqLogID, ok := res.Request.Context().Value(ReqLogIDKey).(ulid.ULID)
		if !ok {
			return errors.New("reqlog: request is missing ID")
		}

		// Use the project ID captured when the request was logged, so the
		// response is stored for the right project even if the active
		// project changed (or was closed) while the request was in flight.
		// Fall back to the current active project for requests that didn't
		// pass through RequestModifier.
		projectID, ok := res.Request.Context().Value(projectIDKey).(ulid.ULID)
		if !ok {
			projectID = svc.ActiveProjectID()
		}

		clone := *res

		if res.Body != nil {
			// TODO: Use io.LimitReader.
			body, err := io.ReadAll(res.Body)
			if err != nil {
				return fmt.Errorf("reqlog: could not read response body: %w", err)
			}

			res.Body = io.NopCloser(bytes.NewBuffer(body))
			clone.Body = io.NopCloser(bytes.NewBuffer(body))
		}

		go func() {
			if err := svc.storeResponse(context.Background(), projectID, reqLogID, &clone); err != nil {
				svc.logger.Errorw("Failed to store response log.",
					"error", err)
			} else {
				svc.logger.Debugw("Stored response log.",
					"reqLogID", reqLogID.String())
			}
		}()

		return nil
	}
}

func (svc *Service) SetActiveProjectID(id ulid.ULID) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	svc.activeProjectID = id
}

func (svc *Service) ActiveProjectID() ulid.ULID {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	return svc.activeProjectID
}

func (svc *Service) SetFindReqsFilter(filter FindRequestsFilter) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	svc.findReqsFilter = filter
}

func (svc *Service) FindReqsFilter() FindRequestsFilter {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	return svc.findReqsFilter
}

func (svc *Service) SetBypassOutOfScopeRequests(bypass bool) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	svc.bypassOutOfScopeRequests = bypass
}

func (svc *Service) BypassOutOfScopeRequests() bool {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	return svc.bypassOutOfScopeRequests
}

func ParseHTTPResponse(res *http.Response) (ResponseLog, error) {
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return ResponseLog{}, fmt.Errorf("reqlog: could not read body: %w", err)
	}

	return ResponseLog{
		Proto:      res.Proto,
		StatusCode: res.StatusCode,
		Status:     res.Status,
		Header:     res.Header,
		Body:       body,
	}, nil
}
