package proj

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"sync"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/filter"
	"github.com/dstotijn/hetty/pkg/proxy/intercept"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/sender"
)

// defaultCloseTimeout bounds how long CloseProject/OpenProject wait for
// in-flight proxied and sender requests to finish before resetting the
// subservices anyway.
const defaultCloseTimeout = 10 * time.Second

//nolint:gosec
var ulidEntropy = rand.New(rand.NewSource(time.Now().UnixNano()))

type Service struct {
	repo         Repository
	interceptSvc *intercept.Service
	reqLogSvc    *reqlog.Service
	senderSvc    *sender.Service
	scope        *scope.Scope

	mu              sync.RWMutex
	activeProjectID ulid.ULID
}

type Project struct {
	ID       ulid.ULID
	Name     string
	Settings Settings

	isActive bool
}

type Settings struct {
	// Request log settings
	ReqLogBypassOutOfScope bool
	ReqLogOnlyFindInScope  bool
	ReqLogSearchExpr       filter.Expression

	// Intercept settings
	InterceptRequests       bool
	InterceptResponses      bool
	InterceptRequestFilter  filter.Expression
	InterceptResponseFilter filter.Expression

	// Sender settings
	SenderOnlyFindInScope bool
	SenderSearchExpr      filter.Expression

	// Scope settings
	ScopeRules []scope.Rule
}

var (
	ErrProjectNotFound  = errors.New("proj: project not found")
	ErrNoProject        = errors.New("proj: no open project")
	ErrNoSettings       = errors.New("proj: settings not found")
	ErrInvalidName      = errors.New("proj: invalid name, must be alphanumeric or whitespace chars")
	ErrCloseProjectWait = errors.New("proj: timed out waiting for in-flight requests while closing project")
)

var nameRegexp = regexp.MustCompile(`^[\w\d\s]+$`)

type Config struct {
	Repository       Repository
	InterceptService *intercept.Service
	ReqLogService    *reqlog.Service
	SenderService    *sender.Service
	Scope            *scope.Scope
}

// NewService returns a new Service.
func NewService(cfg Config) (*Service, error) {
	return &Service{
		repo:         cfg.Repository,
		interceptSvc: cfg.InterceptService,
		reqLogSvc:    cfg.ReqLogService,
		senderSvc:    cfg.SenderService,
		scope:        cfg.Scope,
	}, nil
}

func (svc *Service) CreateProject(ctx context.Context, name string) (Project, error) {
	if !nameRegexp.MatchString(name) {
		return Project{}, ErrInvalidName
	}

	project := Project{
		ID:   ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy),
		Name: name,
	}

	err := svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return Project{}, fmt.Errorf("proj: could not create project: %w", err)
	}

	return project, nil
}

// CloseProject closes the currently open project (if there is one). It
// gracefully drains in-flight proxied and sender requests: pending
// interceptions are aborted, new requests are rejected, and it waits (up to
// defaultCloseTimeout, or until ctx is cancelled) for in-flight work to
// finish before resetting the subservices to an empty state.
func (svc *Service) CloseProject(ctx context.Context) error {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	return svc.closeProjectLocked(ctx)
}

// closeProjectLocked performs the graceful close. The caller must hold
// svc.mu for writing.
func (svc *Service) closeProjectLocked(ctx context.Context) error {
	if svc.activeProjectID.Compare(ulid.ULID{}) == 0 {
		return nil
	}

	svc.activeProjectID = ulid.ULID{}

	// Stop accepting new proxied/sender requests and abort any pending
	// interceptions, unblocking requests that are waiting for user action.
	svc.reqLogSvc.BeginDrain()
	svc.senderSvc.BeginDrain()
	svc.interceptSvc.UpdateSettings(intercept.Settings{
		RequestsEnabled:  false,
		ResponsesEnabled: false,
		RequestFilter:    nil,
		ResponseFilter:   nil,
	})

	// Give in-flight requests a bounded amount of time to finish storing
	// their logs, and cancel early when the caller's context is done.
	waitCtx, cancel := context.WithTimeout(ctx, defaultCloseTimeout)
	defer cancel()

	stop := waitCtx.Done()

	// Wait for both subservices concurrently: a sender request may depend on
	// request log lookup and vice versa, so waiting sequentially could
	// deadlock.
	var reqLogDrained, senderDrained bool

	var drainWg sync.WaitGroup
	drainWg.Add(2)

	go func() {
		defer drainWg.Done()
		reqLogDrained = svc.reqLogSvc.WaitFor(stop)
	}()

	go func() {
		defer drainWg.Done()
		senderDrained = svc.senderSvc.WaitFor(stop)
	}()

	drainWg.Wait()

	// Reset all subservices to an empty project state regardless of whether
	// the drain completed, so the service never ends up half-closed.
	svc.reqLogSvc.SetActiveProjectID(ulid.ULID{})
	svc.reqLogSvc.SetBypassOutOfScopeRequests(false)
	svc.reqLogSvc.SetFindReqsFilter(reqlog.FindRequestsFilter{})
	svc.senderSvc.SetActiveProjectID(ulid.ULID{})
	svc.senderSvc.SetFindReqsFilter(sender.FindRequestsFilter{})
	svc.scope.SetRules(nil)

	if !reqLogDrained || !senderDrained {
		return fmt.Errorf("%w: %v", ErrCloseProjectWait, waitCtx.Err())
	}

	return nil
}

// DeleteProject removes a project from the repository.
func (svc *Service) DeleteProject(ctx context.Context, projectID ulid.ULID) error {
	svc.mu.RLock()
	activeProjectID := svc.activeProjectID
	svc.mu.RUnlock()

	if activeProjectID.Compare(projectID) == 0 {
		return fmt.Errorf("proj: project (%v) is active", projectID.String())
	}

	if err := svc.repo.DeleteProject(ctx, projectID); err != nil {
		return fmt.Errorf("proj: could not delete project: %w", err)
	}

	return nil
}

// OpenProject sets a project as the currently active project. When another
// project is currently open, it is closed gracefully first (see
// CloseProject).
func (svc *Service) OpenProject(ctx context.Context, projectID ulid.ULID) (Project, error) {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	if svc.activeProjectID.Compare(ulid.ULID{}) != 0 {
		if err := svc.closeProjectLocked(ctx); err != nil {
			return Project{}, err
		}
	}

	project, err := svc.repo.FindProjectByID(ctx, projectID)
	if err != nil {
		return Project{}, fmt.Errorf("proj: failed to get project: %w", err)
	}

	svc.activeProjectID = project.ID

	// Request log settings.
	svc.reqLogSvc.SetFindReqsFilter(reqlog.FindRequestsFilter{
		ProjectID:   project.ID,
		OnlyInScope: project.Settings.ReqLogOnlyFindInScope,
		SearchExpr:  project.Settings.ReqLogSearchExpr,
	})
	svc.reqLogSvc.SetBypassOutOfScopeRequests(project.Settings.ReqLogBypassOutOfScope)
	svc.reqLogSvc.SetActiveProjectID(project.ID)

	// Intercept settings.
	svc.interceptSvc.UpdateSettings(intercept.Settings{
		RequestsEnabled:  project.Settings.InterceptRequests,
		ResponsesEnabled: project.Settings.InterceptResponses,
		RequestFilter:    project.Settings.InterceptRequestFilter,
		ResponseFilter:   project.Settings.InterceptResponseFilter,
	})

	// Sender settings.
	svc.senderSvc.SetActiveProjectID(project.ID)
	svc.senderSvc.SetFindReqsFilter(sender.FindRequestsFilter{
		ProjectID:   project.ID,
		OnlyInScope: project.Settings.SenderOnlyFindInScope,
		SearchExpr:  project.Settings.SenderSearchExpr,
	})

	// Scope settings.
	svc.scope.SetRules(project.Settings.ScopeRules)

	return project, nil
}

// ActiveProject returns the currently active project. It holds the service's
// read lock for the whole lookup, so a concurrent CloseProject/OpenProject
// cannot clear the active project ID in between reading the ID and fetching
// the project.
func (svc *Service) ActiveProject(ctx context.Context) (Project, error) {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	if svc.activeProjectID.Compare(ulid.ULID{}) == 0 {
		return Project{}, ErrNoProject
	}

	project, err := svc.repo.FindProjectByID(ctx, svc.activeProjectID)
	if err != nil {
		return Project{}, fmt.Errorf("proj: failed to get active project: %w", err)
	}

	project.isActive = true

	return project, nil
}

// activeProjectLocked returns the active project under the caller's lock
// (write or read). Settings updates use it together with UpsertProject while
// holding the write lock, making each read-modify-write cycle atomic.
func (svc *Service) activeProjectLocked(ctx context.Context) (Project, error) {
	if svc.activeProjectID.Compare(ulid.ULID{}) == 0 {
		return Project{}, ErrNoProject
	}

	project, err := svc.repo.FindProjectByID(ctx, svc.activeProjectID)
	if err != nil {
		return Project{}, fmt.Errorf("proj: failed to get active project: %w", err)
	}

	project.isActive = true

	return project, nil
}

func (svc *Service) Projects(ctx context.Context) ([]Project, error) {
	projects, err := svc.repo.Projects(ctx)
	if err != nil {
		return nil, fmt.Errorf("proj: could not get projects: %w", err)
	}

	return projects, nil
}

func (svc *Service) Scope() *scope.Scope {
	return svc.scope
}

func (svc *Service) SetScopeRules(ctx context.Context, rules []scope.Rule) error {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	project, err := svc.activeProjectLocked(ctx)
	if err != nil {
		return err
	}

	project.Settings.ScopeRules = rules

	err = svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return fmt.Errorf("proj: failed to update project: %w", err)
	}

	svc.scope.SetRules(rules)

	return nil
}

func (svc *Service) SetRequestLogFindFilter(ctx context.Context, filter reqlog.FindRequestsFilter) error {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	project, err := svc.activeProjectLocked(ctx)
	if err != nil {
		return err
	}

	filter.ProjectID = project.ID

	project.Settings.ReqLogOnlyFindInScope = filter.OnlyInScope
	project.Settings.ReqLogSearchExpr = filter.SearchExpr

	err = svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return fmt.Errorf("proj: failed to update project: %w", err)
	}

	svc.reqLogSvc.SetFindReqsFilter(filter)

	return nil
}

func (svc *Service) SetSenderRequestFindFilter(ctx context.Context, filter sender.FindRequestsFilter) error {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	project, err := svc.activeProjectLocked(ctx)
	if err != nil {
		return err
	}

	filter.ProjectID = project.ID

	project.Settings.SenderOnlyFindInScope = filter.OnlyInScope

	project.Settings.SenderSearchExpr = filter.SearchExpr

	err = svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return fmt.Errorf("proj: failed to update project: %w", err)
	}

	svc.senderSvc.SetFindReqsFilter(filter)

	return nil
}

func (svc *Service) IsProjectActive(projectID ulid.ULID) bool {
	svc.mu.RLock()
	defer svc.mu.RUnlock()

	return projectID.Compare(svc.activeProjectID) == 0
}

func (svc *Service) UpdateInterceptSettings(ctx context.Context, settings intercept.Settings) error {
	svc.mu.Lock()
	defer svc.mu.Unlock()

	project, err := svc.activeProjectLocked(ctx)
	if err != nil {
		return err
	}

	project.Settings.InterceptRequests = settings.RequestsEnabled
	project.Settings.InterceptResponses = settings.ResponsesEnabled
	project.Settings.InterceptRequestFilter = settings.RequestFilter
	project.Settings.InterceptResponseFilter = settings.ResponseFilter

	err = svc.repo.UpsertProject(ctx, project)
	if err != nil {
		return fmt.Errorf("proj: failed to update project: %w", err)
	}

	svc.interceptSvc.UpdateSettings(settings)

	return nil
}
