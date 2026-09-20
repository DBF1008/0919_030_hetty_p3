package proj_test

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"

	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/proxy/intercept"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/sender"
)

//nolint:gosec
var ulidEntropy = rand.New(rand.NewSource(time.Now().UnixNano()))

// mockRepository is an in-memory proj.Repository implementation.
type mockRepository struct {
	mu       sync.Mutex
	delay    time.Duration
	projects map[ulid.ULID]proj.Project
}

func newMockRepository() *mockRepository {
	return &mockRepository{projects: make(map[ulid.ULID]proj.Project)}
}

func (r *mockRepository) FindProjectByID(_ context.Context, id ulid.ULID) (proj.Project, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.delay > 0 {
		time.Sleep(r.delay)
	}

	project, ok := r.projects[id]
	if !ok {
		return proj.Project{}, proj.ErrProjectNotFound
	}

	return project, nil
}

func (r *mockRepository) UpsertProject(_ context.Context, project proj.Project) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.delay > 0 {
		time.Sleep(r.delay)
	}

	r.projects[project.ID] = project

	return nil
}

func (r *mockRepository) DeleteProject(_ context.Context, id ulid.ULID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.projects, id)

	return nil
}

func (r *mockRepository) Projects(_ context.Context) ([]proj.Project, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	projects := make([]proj.Project, 0, len(r.projects))
	for _, project := range r.projects {
		projects = append(projects, project)
	}

	return projects, nil
}

func (r *mockRepository) Close() error { return nil }

type testServices struct {
	projSvc      *proj.Service
	repo         *mockRepository
	interceptSvc *intercept.Service
	reqLogSvc    *reqlog.Service
	senderSvc    *sender.Service
	scope        *scope.Scope
}

func newTestServices(t *testing.T) *testServices {
	t.Helper()

	repo := newMockRepository()
	scp := &scope.Scope{}
	interceptSvc := intercept.NewService(intercept.Config{})
	reqLogSvc := reqlog.NewService(reqlog.Config{Scope: scp})
	senderSvc := sender.NewService(sender.Config{Scope: scp})

	projSvc, err := proj.NewService(proj.Config{
		Repository:       repo,
		InterceptService: interceptSvc,
		ReqLogService:    reqLogSvc,
		SenderService:    senderSvc,
		Scope:            scp,
	})
	if err != nil {
		t.Fatalf("failed to create proj service: %v", err)
	}

	return &testServices{
		projSvc:      projSvc,
		repo:         repo,
		interceptSvc: interceptSvc,
		reqLogSvc:    reqLogSvc,
		senderSvc:    senderSvc,
		scope:        scp,
	}
}

func (ts *testServices) createAndOpenProject(t *testing.T, name string) proj.Project {
	t.Helper()

	project, err := ts.projSvc.CreateProject(context.Background(), name)
	if err != nil {
		t.Fatalf("failed to create project: %v", err)
	}

	if _, err := ts.projSvc.OpenProject(context.Background(), project.ID); err != nil {
		t.Fatalf("failed to open project: %v", err)
	}

	return project
}

func TestActiveProjectWithoutOpenProject(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)

	_, err := ts.projSvc.ActiveProject(context.Background())
	if !errors.Is(err, proj.ErrNoProject) {
		t.Fatalf("expected `proj.ErrNoProject`, got: %v", err)
	}
}

func TestOpenProjectAppliesSettingsToSubServices(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)
	ctx := context.Background()

	project, err := ts.projSvc.CreateProject(ctx, "test project")
	if err != nil {
		t.Fatalf("failed to create project: %v", err)
	}

	project.Settings.ReqLogBypassOutOfScope = true
	project.Settings.ReqLogOnlyFindInScope = true
	project.Settings.ScopeRules = []scope.Rule{{URL: regexp.MustCompile(`example\.com`)}}
	project.Settings.SenderOnlyFindInScope = true

	if err := ts.repo.UpsertProject(ctx, project); err != nil {
		t.Fatalf("failed to update project: %v", err)
	}

	if _, err := ts.projSvc.OpenProject(ctx, project.ID); err != nil {
		t.Fatalf("failed to open project: %v", err)
	}

	if !ts.projSvc.IsProjectActive(project.ID) {
		t.Fatal("expected project to be active")
	}

	if got := ts.reqLogSvc.ActiveProjectID(); got != project.ID {
		t.Fatalf("reqlog active project ID = %v, want %v", got, project.ID)
	}

	if !ts.reqLogSvc.BypassOutOfScopeRequests() {
		t.Fatal("expected reqlog BypassOutOfScopeRequests to be true")
	}

	if got := ts.reqLogSvc.FindReqsFilter(); got.ProjectID != project.ID || !got.OnlyInScope {
		t.Fatalf("unexpected reqlog find filter: %+v", got)
	}

	if got := ts.senderSvc.FindReqsFilter(); got.ProjectID != project.ID || !got.OnlyInScope {
		t.Fatalf("unexpected sender find filter: %+v", got)
	}

	if got := ts.scope.Rules(); len(got) != 1 {
		t.Fatalf("expected 1 scope rule, got %d", len(got))
	}

	active, err := ts.projSvc.ActiveProject(ctx)
	if err != nil {
		t.Fatalf("failed to get active project: %v", err)
	}

	if active.ID != project.ID {
		t.Fatalf("active project ID = %v, want %v", active.ID, project.ID)
	}
}

func TestCloseProjectResetsSubServices(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)
	ctx := context.Background()

	project := ts.createAndOpenProject(t, "test project")

	if err := ts.projSvc.SetScopeRules(ctx, []scope.Rule{{URL: regexp.MustCompile(`example\.com`)}}); err != nil {
		t.Fatalf("failed to set scope rules: %v", err)
	}

	if err := ts.projSvc.CloseProject(); err != nil {
		t.Fatalf("failed to close project: %v", err)
	}

	if _, err := ts.projSvc.ActiveProject(ctx); !errors.Is(err, proj.ErrNoProject) {
		t.Fatalf("expected `proj.ErrNoProject`, got: %v", err)
	}

	if ts.projSvc.IsProjectActive(project.ID) {
		t.Fatal("expected project to no longer be active")
	}

	if got := ts.reqLogSvc.ActiveProjectID(); got.Compare(ulid.ULID{}) != 0 {
		t.Fatalf("expected reqlog active project ID to be reset, got: %v", got)
	}

	if got := ts.scope.Rules(); len(got) != 0 {
		t.Fatalf("expected scope rules to be reset, got: %v", got)
	}

	if got := ts.senderSvc.FindReqsFilter(); got.ProjectID.Compare(ulid.ULID{}) != 0 {
		t.Fatalf("expected sender find filter to be reset, got: %+v", got)
	}
}

func TestCloseProjectWithoutOpenProject(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)

	if err := ts.projSvc.CloseProject(); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestSettingsSettersRequireOpenProject(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)
	ctx := context.Background()

	if err := ts.projSvc.UpdateInterceptSettings(ctx, intercept.Settings{RequestsEnabled: true}); !errors.Is(err, proj.ErrNoProject) {
		t.Fatalf("UpdateInterceptSettings: expected `proj.ErrNoProject`, got: %v", err)
	}

	if err := ts.projSvc.SetScopeRules(ctx, nil); !errors.Is(err, proj.ErrNoProject) {
		t.Fatalf("SetScopeRules: expected `proj.ErrNoProject`, got: %v", err)
	}

	if err := ts.projSvc.SetRequestLogFindFilter(ctx, reqlog.FindRequestsFilter{}); !errors.Is(err, proj.ErrNoProject) {
		t.Fatalf("SetRequestLogFindFilter: expected `proj.ErrNoProject`, got: %v", err)
	}

	if err := ts.projSvc.SetSenderRequestFindFilter(ctx, sender.FindRequestsFilter{}); !errors.Is(err, proj.ErrNoProject) {
		t.Fatalf("SetSenderRequestFindFilter: expected `proj.ErrNoProject`, got: %v", err)
	}
}

func TestUpdateInterceptSettingsPersists(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)
	ctx := context.Background()

	ts.createAndOpenProject(t, "test project")

	settings := intercept.Settings{
		RequestsEnabled:  true,
		ResponsesEnabled: true,
	}
	if err := ts.projSvc.UpdateInterceptSettings(ctx, settings); err != nil {
		t.Fatalf("failed to update intercept settings: %v", err)
	}

	active, err := ts.projSvc.ActiveProject(ctx)
	if err != nil {
		t.Fatalf("failed to get active project: %v", err)
	}

	if !active.Settings.InterceptRequests || !active.Settings.InterceptResponses {
		t.Fatalf("intercept settings were not persisted: %+v", active.Settings)
	}
}

// TestConcurrentSettingsUpdatesAreAtomic verifies that concurrent settings
// updates (read-modify-write cycles) don't overwrite each other.
func TestConcurrentSettingsUpdatesAreAtomic(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)
	// Slow down the repository to widen the race window.
	ts.repo.delay = 10 * time.Millisecond

	ctx := context.Background()
	ts.createAndOpenProject(t, "test project")

	rules := []scope.Rule{{URL: regexp.MustCompile(`example\.com`)}}

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		if err := ts.projSvc.UpdateInterceptSettings(ctx, intercept.Settings{RequestsEnabled: true}); err != nil {
			t.Errorf("UpdateInterceptSettings failed: %v", err)
		}
	}()

	go func() {
		defer wg.Done()

		if err := ts.projSvc.SetScopeRules(ctx, rules); err != nil {
			t.Errorf("SetScopeRules failed: %v", err)
		}
	}()

	wg.Wait()

	active, err := ts.projSvc.ActiveProject(ctx)
	if err != nil {
		t.Fatalf("failed to get active project: %v", err)
	}

	if !active.Settings.InterceptRequests {
		t.Fatal("intercept settings update was lost by a concurrent update")
	}

	if len(active.Settings.ScopeRules) != 1 {
		t.Fatal("scope rules update was lost by a concurrent update")
	}
}

// TestConcurrentOpenCloseAndUpdates hammers the service with concurrent
// open/close/read/update operations. It's primarily useful with the race
// detector enabled (`go test -race`).
func TestConcurrentOpenCloseAndUpdates(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)
	ctx := context.Background()

	projectA, err := ts.projSvc.CreateProject(ctx, "project a")
	if err != nil {
		t.Fatalf("failed to create project: %v", err)
	}

	projectB, err := ts.projSvc.CreateProject(ctx, "project b")
	if err != nil {
		t.Fatalf("failed to create project: %v", err)
	}

	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			projectID := projectA.ID
			if i%2 == 0 {
				projectID = projectB.ID
			}

			for j := 0; j < 25; j++ {
				//nolint:errcheck
				ts.projSvc.OpenProject(ctx, projectID)
				//nolint:errcheck
				ts.projSvc.ActiveProject(ctx)
				//nolint:errcheck
				ts.projSvc.UpdateInterceptSettings(ctx, intercept.Settings{RequestsEnabled: j%2 == 0})
				//nolint:errcheck
				ts.projSvc.SetScopeRules(ctx, []scope.Rule{{URL: regexp.MustCompile(`example\.com`)}})
				//nolint:errcheck
				ts.projSvc.SetRequestLogFindFilter(ctx, reqlog.FindRequestsFilter{OnlyInScope: true})
				//nolint:errcheck
				ts.projSvc.SetSenderRequestFindFilter(ctx, sender.FindRequestsFilter{OnlyInScope: true})
				ts.projSvc.IsProjectActive(projectID)

				if j%3 == 0 {
					//nolint:errcheck
					ts.projSvc.CloseProject()
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestCloseProjectDrainsPendingIntercept verifies that CloseProject aborts
// pending intercepted requests and waits for their handlers to finish.
func TestCloseProjectDrainsPendingIntercept(t *testing.T) {
	t.Parallel()

	ts := newTestServices(t)
	ctx := context.Background()

	ts.createAndOpenProject(t, "test project")

	if err := ts.projSvc.UpdateInterceptSettings(ctx, intercept.Settings{RequestsEnabled: true}); err != nil {
		t.Fatalf("failed to enable request interception: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	interceptErr := make(chan error, 1)

	go func() {
		_, err := ts.interceptSvc.InterceptRequest(req.Context(), req)
		interceptErr <- err
	}()

	// Wait until the request is pending in the intercept queue.
	deadline := time.Now().Add(2 * time.Second)
	for len(ts.interceptSvc.Items()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("request was not intercepted")
		}

		time.Sleep(time.Millisecond)
	}

	closeDone := make(chan error, 1)

	go func() {
		closeDone <- ts.projSvc.CloseProject()
	}()

	select {
	case err := <-interceptErr:
		if !errors.Is(err, intercept.ErrRequestAborted) {
			t.Fatalf("expected `intercept.ErrRequestAborted`, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending intercepted request was not aborted on CloseProject")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("CloseProject returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CloseProject did not return; graceful drain failed")
	}

	if _, err := ts.projSvc.ActiveProject(ctx); !errors.Is(err, proj.ErrNoProject) {
		t.Fatalf("expected `proj.ErrNoProject` after close, got: %v", err)
	}
}
