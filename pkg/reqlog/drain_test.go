package reqlog_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
)

func newDrainService(t *testing.T) (*reqlog.Service, *intercept.Service, *bolt.Database, ulid.ULID) {
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
	interceptSvc := intercept.NewService(intercept.Config{})

	projectID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	if err := db.UpsertProject(context.Background(), proj.Project{ID: projectID}); err != nil {
		t.Fatalf("failed to upsert project: %v", err)
	}

	reqLogSvc.SetActiveProjectID(projectID)

	return reqLogSvc, interceptSvc, db, projectID
}

func chainedRequestModifier(reqLogSvc *reqlog.Service, interceptSvc *intercept.Service) proxy.RequestModifyFunc {
	// Outer middleware is reqlog, inner middleware is intercept: this matches
	// the registration order in cmd/hetty (request modifiers are wrapped in
	// reverse registration order).
	return reqLogSvc.RequestModifier(interceptSvc.RequestModifier(func(req *http.Request) {}))
}

// TestBeginDrainRejectsNewRequests verifies that after BeginDrain, the request
// modifier rejects new requests (cancels them) instead of logging them.
func TestBeginDrainRejectsNewRequests(t *testing.T) {
	reqLogSvc, _, _, _ := newDrainService(t)

	reqLogSvc.BeginDrain()

	req := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	reqLogSvc.RequestModifier(func(*http.Request) {})(req)

	select {
	case <-req.Context().Done():
		// Expected: the request was rejected with a cancelled context.
	default:
		t.Fatal("expected request context to be cancelled while draining")
	}
}

// TestDrainWaitsForQueuedIntercept verifies a request waiting in the
// interception queue keeps the drain pending until it is unblocked, after
// which the request is still logged against its original project.
func TestDrainWaitsForQueuedIntercept(t *testing.T) {
	reqLogSvc, interceptSvc, db, _ := newDrainService(t)
	interceptSvc.UpdateSettings(intercept.Settings{RequestsEnabled: true})

	requestModifier := chainedRequestModifier(reqLogSvc, interceptSvc)

	req := httptest.NewRequest(http.MethodGet, "https://example.com/", strings.NewReader("foo"))
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

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
		t.Fatal("expected request to be queued for interception")
	}

	reqLogSvc.BeginDrain()

	drainReturned := make(chan struct{})

	go func() {
		reqLogSvc.Wait()
		close(drainReturned)
	}()

	select {
	case <-drainReturned:
		t.Fatal("expected drain to wait while request is queued in interception")
	case <-time.After(50 * time.Millisecond):
	}

	// Release the request unmodified.
	modReq := req.Clone(req.Context())
	if err := interceptSvc.ModifyRequest(reqID, modReq, nil); err != nil {
		t.Fatalf("failed to modify request: %v", err)
	}

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("expected request modifier to finish")
	}

	select {
	case <-drainReturned:
	case <-time.After(time.Second):
		t.Fatal("expected drain to finish after queued request was released")
	}

	// The request log was stored for the active project.
	_, err := db.FindRequestLogByID(context.Background(), reqLogSvc.ActiveProjectID(), reqID)
	if err != nil {
		t.Fatalf("expected stored request log: %v", err)
	}
}

// TestDrainResponseLogIsStored verifies a response that starts processing
// before the drain and is stored while draining keeps the drain pending until
// the response log is persisted.
func TestDrainResponseLogIsStored(t *testing.T) {
	reqLogSvc, interceptSvc, db, projectID := newDrainService(t)

	requestModifier := chainedRequestModifier(reqLogSvc, interceptSvc)

	req := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	reqID := ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy)
	req = req.WithContext(proxy.WithRequestID(req.Context(), reqID))

	requestModifier(req)

	if _, err := db.FindRequestLogByID(context.Background(), projectID, reqID); err != nil {
		t.Fatalf("expected stored request log: %v", err)
	}

	res := &http.Response{
		Request: req,
		Body:    io.NopCloser(strings.NewReader("response body")),
	}

	// Begin draining right before processing the response.
	reqLogSvc.BeginDrain()

	drainReturned := make(chan struct{})

	go func() {
		reqLogSvc.Wait()
		close(drainReturned)
	}()

	responseModifier := reqLogSvc.ResponseModifier(func(*http.Response) error { return nil })
	if err := responseModifier(res); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-drainReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("expected drain to wait for the response log to be stored")
	}

	got, err := db.FindRequestLogByID(context.Background(), projectID, reqID)
	if err != nil {
		t.Fatalf("failed to find request log by id: %v", err)
	}

	if got.Response == nil || string(got.Response.Body) != "response body" {
		t.Fatalf("expected response log to be stored, got: %#v", got.Response)
	}
}
