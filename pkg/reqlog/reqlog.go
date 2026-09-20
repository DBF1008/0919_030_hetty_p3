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
	"github.com/dstotijn/hetty/pkg/proxy/intercept"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/syncutil"
)

type contextKey int

const (
	LogBypassedKey contextKey = iota
	ReqLogIDKey
	reqLogProjectIDKey
	inflightTokenKey
)

var (
	ErrRequestNotFound    = errors.New("reqlog: request not found")
	ErrProjectIDMustBeSet = errors.New("reqlog: project ID must be set")
	ErrServiceClosing     = errors.New("reqlog: service is closing")
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

// inflightToken represents one in-flight proxied request. It is created in the
// request modifier and released exactly once: either by the response modifier
// (handed over through the request context), or when the request context is
// cancelled before a response is processed.
type inflightToken struct {
	inflight *syncutil.InFlight
	// stopCancel releases the token when the request context is cancelled,
	// until ownership of the token is handed over to the response modifier.
	stopCancel func() bool
	once       sync.Once
}

func (t *inflightToken) release() {
	t.once.Do(func() {
		if t.stopCancel != nil {
			t.stopCancel()
		}

		t.inflight.Done()
	})
}

type Service struct {
	mu sync.RWMutex

	bypassOutOfScopeRequests bool
	findReqsFilter           FindRequestsFilter
	activeProjectID          ulid.ULID

	inflight *syncutil.InFlight

	scope  *scope.Scope
	repo   Repository
	logger log.Logger
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
		inflight:        syncutil.NewInFlight(),
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
	svc.mu.RLock()
	projectID := svc.activeProjectID
	svc.mu.RUnlock()

	return svc.repo.FindRequestLogByID(ctx, projectID, id)
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
		// Register the whole proxied request (including any blocking request
		// interception happening in an outer modifier) as in-flight. While the
		// service is draining, new requests are rejected instead of being
		// logged half-way through a project close.
		if !svc.inflight.Begin() {
			svc.logger.Debugw("Rejecting request: service is closing.",
				"url", req.URL.String())

			ctx, cancel := context.WithCancelCause(context.Background())
			cancel(ErrServiceClosing)
			*req = *req.WithContext(ctx)

			return
		}

		token := &inflightToken{inflight: svc.inflight}
		defer token.release()

		// Release the token as soon as the proxied request is cancelled (for
		// example when the client disconnects or request interception aborts
		// it), in which case ResponseModifier never runs.
		token.stopCancel = context.AfterFunc(req.Context(), token.release)

		// Request interception invokes this hook synchronously when it aborts
		// the request, so a queued abort doesn't have to wait for the request
		// context cancellation to propagate during a graceful close.
		*req = *req.WithContext(intercept.WithAbortHook(req.Context(), token.release))

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
		activeProjectID := svc.activeProjectID
		bypassOutOfScopeRequests := svc.bypassOutOfScopeRequests
		svc.mu.RUnlock()

		// Bypass logging if no project is active.
		if activeProjectID.Compare(ulid.ULID{}) == 0 {
			ctx := context.WithValue(req.Context(), LogBypassedKey, true)
			*req = *req.WithContext(ctx)

			svc.logger.Debugw("Bypassed logging: no active project.",
				"url", req.URL.String())

			return
		}

		// Bypass logging if this setting is enabled and the incoming request
		// doesn't match any scope rules.
		if bypassOutOfScopeRequests && !svc.scope.Match(clone, body) {
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
			ProjectID: activeProjectID,
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

		// Hand the in-flight token and the project ID over to the response
		// modifier, so response logs are stored for the project that was
		// active when the request was logged, even if it is closed meanwhile.
		ctx := context.WithValue(req.Context(), ReqLogIDKey, reqLog.ID)
		ctx = context.WithValue(ctx, reqLogProjectIDKey, activeProjectID)
		ctx = context.WithValue(ctx, inflightTokenKey, token)
		*req = *req.WithContext(ctx)

		// The token is now owned by the response modifier (or released when
		// the request context is cancelled).
		token.stopCancel = nil
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

		projectID, _ := res.Request.Context().Value(reqLogProjectIDKey).(ulid.ULID)
		if projectID.Compare(ulid.ULID{}) == 0 {
			// Fallback for response modifiers invoked without a preceding
			// request modifier (e.g. in tests).
			svc.mu.RLock()
			projectID = svc.activeProjectID
			svc.mu.RUnlock()
		}

		// Take ownership of the request modifier's in-flight token, if any.
		// It is released in every exit path below (synchronously while
		// draining, or by the background goroutine otherwise).
		token, _ := res.Request.Context().Value(inflightTokenKey).(*inflightToken)

		releaseToken := func() {
			if token != nil {
				token.release()
			}
		}

		clone := *res

		if res.Body != nil {
			// TODO: Use io.LimitReader.
			body, err := io.ReadAll(res.Body)
			if err != nil {
				releaseToken()
				return fmt.Errorf("reqlog: could not read response body: %w", err)
			}

			res.Body = io.NopCloser(bytes.NewBuffer(body))
			clone.Body = io.NopCloser(bytes.NewBuffer(body))
		}

		// When the service is draining, new background work must not be
		// registered: wait for the response log to be stored synchronously so
		// a graceful close never loses in-flight logs.
		if !svc.inflight.Begin() {
			if err := svc.storeResponse(context.Background(), projectID, reqLogID, &clone); err != nil {
				svc.logger.Errorw("Failed to store response log.",
					"error", err)
			} else {
				svc.logger.Debugw("Stored response log during drain.",
					"reqLogID", reqLogID.String())
			}

			releaseToken()

			return nil
		}
		drainToken := true
		defer func() {
			if drainToken {
				svc.inflight.Done()
			}
		}()

		go func() {
			// Both tokens are released after the response log is stored.
			defer svc.inflight.Done()
			defer releaseToken()

			if err := svc.storeResponse(context.Background(), projectID, reqLogID, &clone); err != nil {
				svc.logger.Errorw("Failed to store response log.",
					"error", err)
			} else {
				svc.logger.Debugw("Stored response log.",
					"reqLogID", reqLogID.String())
			}
		}()

		// Ownership of the goroutine token and the request token is handed to
		// the background goroutine.
		drainToken = false

		return nil
	}
}

// BeginDrain rejects new proxied requests, so that a graceful project close
// doesn't race with in-flight logging.
func (svc *Service) BeginDrain() {
	svc.inflight.BeginDrain()
}

// Wait blocks until all in-flight request logging has finished.
func (svc *Service) Wait() {
	svc.inflight.Wait()
}

// WaitFor waits for all in-flight request logging to finish, or until stop is
// closed. It returns true when all work has finished.
func (svc *Service) WaitFor(stop <-chan struct{}) bool {
	return svc.inflight.WaitFor(stop)
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
