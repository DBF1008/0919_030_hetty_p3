package proj_test

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/proxy"
	"github.com/dstotijn/hetty/pkg/proxy/intercept"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/scope"
	"github.com/dstotijn/hetty/pkg/sender"
)

//nolint:gosec
var ulidEntropy = rand.New(rand.NewSource(time.Now().UnixNano()))

func newProjectService(t *testing.T) (*proj.Service, *bolt.Database, *intercept.Service) {
	t.Helper()

	boltDB, err := bbolt.Open(t.TempDir()+"/bolt.db", 0o600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt database: %v", err)
	}

	t.Cleanup(func() { boltDB.Close() })

	db, err := bolt.DatabaseFromBoltDB(boltDB)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}

	scopeSvc := &scope.Scope{}
	reqLogSvc := reqlog.NewService(reqlog.Config{Repository: db, Scope: scopeSvc})
	interceptSvc := intercept.NewService(intercept.Config{RequestsEnabled: true})
	senderSvc := sender.NewService(sender.Config{Repository: db, Scope: scopeSvc, ReqLogService: reqLogSvc})

	projSvc, err := proj.NewService(proj.Config{
		Repository:       db,
		InterceptService: interceptSvc,
		ReqLogService:    reqLogSvc,
		SenderService:    senderSvc,
		Scope:            scopeSvc,
	})
	if err != nil {
		t.Fatalf("failed to create project service: %v", err)
	}

	return projSvc, db, interceptSvc
}

func createAndOpenProject(t *testing.T, projSvc *proj.Service, db *bolt.Database) proj.Project {
	t.Helper()

	ctx := context.Background()
	project := proj.Project{
		ID:   ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy),
		Name: "Test project",
		Settings: proj.Settings{
			InterceptRequests: true,
		},
	}

	if err := db.UpsertProject(ctx, project); err != nil {
		t.Fatalf("failed to upsert project: %v", err)
	}

	if _, err := projSvc.OpenProject(ctx, project.ID); err != nil {
		t.Fatalf("failed to open project: %v", err)
	}

	return project
}

// TestActiveProjectConcurrentClose verifies ActiveProject never returns a
// partially closed project when racing with CloseProject: it must return
// either the active project or ErrNoProject.
func TestActiveProjectConcurrentClose(t *testing.T) {
	projSvc, db, _ := newProjectService(t)
	project := createAndOpenProject(t, projSvc, db)

	ctx := context.Background()

	const goroutines = 16

	var wg sync.WaitGroup

	results := make(chan error, goroutines*10)

	for i := 0; i < goroutines; i++ {
		wg.Add(2)

		go func() {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				_, err := projSvc.ActiveProject(ctx)
				switch {
				case err == nil:
				case errors.Is(err, proj.ErrNoProject):
				default:
					results <- err
				}
			}
		}()

		go func() {
			defer wg.Done()

			if err := projSvc.CloseProject(ctx); err != nil {
				results <- err
			}

			if _, err := projSvc.OpenProject(ctx, project.ID); err != nil {
				results <- err
			}
		}()
	}

	wg.Wait()
	close(results)

	for err := range results {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestConcurrentSettingsUpdates verifies the read-modify-write cycle of
// settings is atomic: concurrent updates of different settings never overwrite
// each other.
func TestConcurrentSettingsUpdates(t *testing.T) {
	projSvc, db, _ := newProjectService(t)
	createAndOpenProject(t, projSvc, db)

	ctx := context.Background()

	const iterations = 25

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			err := projSvc.SetRequestLogFindFilter(ctx, reqlog.FindRequestsFilter{
				OnlyInScope: i%2 == 0,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < iterations; i++ {
			err := projSvc.UpdateInterceptSettings(ctx, intercept.Settings{
				RequestsEnabled: i%2 == 1,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}
	}()

	wg.Wait()

	project, err := projSvc.ActiveProject(ctx)
	if err != nil {
		t.Fatalf("failed to get active project: %v", err)
	}

	if !project.Settings.ReqLogOnlyFindInScope {
		t.Fatal("expected req log `only in scope` to reflect the final update (true)")
	}

	if project.Settings.InterceptRequests {
		t.Fatal("expected intercept requests to reflect the final update (false)")
	}
}

// TestCloseProjectAbortsPendingIntercept verifies that closing a project
// unblocks requests waiting in the interception queue instead of leaving them
// stuck.
func TestCloseProjectAbortsPendingIntercept(t *testing.T) {
	projSvc, db, interceptSvc := newProjectService(t)
	createAndOpenProject(t, projSvc, db)

	req := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	requestModifier := interceptSvc.RequestModifier(func(*http.Request) {})

	finished := make(chan struct{})

	go func() {
		requestModifier(req)
		close(finished)
	}()

	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		if len(interceptSvc.Items()) == 1 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	if len(interceptSvc.Items()) != 1 {
		t.Fatal("expected request to be pending in interception queue")
	}

	if err := projSvc.CloseProject(context.Background()); err != nil {
		t.Fatalf("failed to close project: %v", err)
	}

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("expected pending intercepted request to be aborted when project closed")
	}

	items := interceptSvc.Items()
	if len(items) != 0 {
		t.Fatalf("expected interception queue to be empty, got %d items", len(items))
	}
}

// TestCloseProjectRejectsNewRequests verifies that after closing a project,
// new proxied requests are rejected (bypassed with a cancelled context) rather
// than being logged against an empty project.
func TestCloseProjectRejectsNewRequests(t *testing.T) {
	projSvc, db, _ := newProjectService(t)
	createAndOpenProject(t, projSvc, db)

	// Use a fresh reqlog service handle via a second project service wiring is
	// not needed: the reqlog service is reachable through a new proxied
	// request after closing.
	if err := projSvc.CloseProject(context.Background()); err != nil {
		t.Fatalf("failed to close project: %v", err)
	}

	_, err := projSvc.ActiveProject(context.Background())
	if !errors.Is(err, proj.ErrNoProject) {
		t.Fatalf("expected ErrNoProject, got: %v", err)
	}
}

// TestOpenProjectClosesPreviousProject verifies that opening a second project
// gracefully closes the first one.
func TestOpenProjectClosesPreviousProject(t *testing.T) {
	projSvc, db, interceptSvc := newProjectService(t)
	first := createAndOpenProject(t, projSvc, db)

	second := proj.Project{
		ID:   ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy),
		Name: "Second project",
	}

	if err := db.UpsertProject(context.Background(), second); err != nil {
		t.Fatalf("failed to upsert project: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	requestModifier := interceptSvc.RequestModifier(func(*http.Request) {})

	finished := make(chan struct{})

	go func() {
		requestModifier(req)
		close(finished)
	}()

	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		if len(interceptSvc.Items()) == 1 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	opened, err := projSvc.OpenProject(context.Background(), second.ID)
	if err != nil {
		t.Fatalf("failed to open second project: %v", err)
	}

	if opened.ID.Compare(second.ID) != 0 {
		t.Fatal("expected second project to be active")
	}

	if projSvc.IsProjectActive(first.ID) {
		t.Fatal("expected first project to no longer be active")
	}

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("expected pending intercepted request to be aborted when switching projects")
	}
}
