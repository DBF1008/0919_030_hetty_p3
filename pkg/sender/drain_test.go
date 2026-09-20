package sender_test

import (
	"context"
	"errors"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"go.etcd.io/bbolt"

	"github.com/dstotijn/hetty/pkg/db/bolt"
	"github.com/dstotijn/hetty/pkg/proj"
	"github.com/dstotijn/hetty/pkg/reqlog"
	"github.com/dstotijn/hetty/pkg/sender"
)

//nolint:gosec
var drainUlidEntropy = rand.New(rand.NewSource(time.Now().UnixNano()))

func drainULID() ulid.ULID {
	return ulid.MustNew(ulid.Timestamp(time.Now()), drainUlidEntropy)
}

func newSenderDrainService(t *testing.T) (*sender.Service, ulid.ULID, *bolt.Database) {
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

	reqLogSvc := reqlog.NewService(reqlog.Config{Repository: db})
	svc := sender.NewService(sender.Config{Repository: db, ReqLogService: reqLogSvc})

	projectID := drainULID()
	if err := db.UpsertProject(context.Background(), proj.Project{ID: projectID}); err != nil {
		t.Fatalf("failed to upsert project: %v", err)
	}

	svc.SetActiveProjectID(projectID)

	return svc, projectID, db
}

// TestSenderRejectsWorkWhileDraining verifies sender operations fail with
// ErrServiceClosing once a drain has started, instead of writing against a
// project that is being closed.
func TestSenderRejectsWorkWhileDraining(t *testing.T) {
	svc, projectID, db := newSenderDrainService(t)

	reqID := drainULID()
	targetURL, _ := url.Parse("https://example.com/")

	if err := db.StoreSenderRequest(context.Background(), sender.Request{
		ID:        reqID,
		ProjectID: projectID,
		URL:       targetURL,
		Method:    "GET",
		Proto:     "HTTP/1.1",
	}); err != nil {
		t.Fatalf("failed to store request: %v", err)
	}

	svc.BeginDrain()

	if _, err := svc.CreateOrUpdateRequest(context.Background(), sender.Request{URL: targetURL}); !errors.Is(err, sender.ErrServiceClosing) {
		t.Fatalf("expected ErrServiceClosing from CreateOrUpdateRequest, got: %v", err)
	}

	if _, err := svc.CloneFromRequestLog(context.Background(), drainULID()); !errors.Is(err, sender.ErrServiceClosing) {
		t.Fatalf("expected ErrServiceClosing from CloneFromRequestLog, got: %v", err)
	}

	if _, err := svc.SendRequest(context.Background(), reqID); !errors.Is(err, sender.ErrServiceClosing) {
		t.Fatalf("expected ErrServiceClosing from SendRequest, got: %v", err)
	}
}

// TestSenderDrainCompletesWhenIdle verifies Wait returns immediately when no
// requests are in flight and the tracker can be reused afterwards.
func TestSenderDrainCompletesWhenIdle(t *testing.T) {
	svc, _, _ := newSenderDrainService(t)

	svc.BeginDrain()

	done := make(chan struct{})

	go func() {
		svc.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected Wait to return when idle")
	}

	if _, err := svc.CreateOrUpdateRequest(context.Background(), sender.Request{
		URL:    mustParseURL(t, "https://example.com/"),
		Method: "GET",
		Proto:  "HTTP/1.1",
	}); err != nil {
		t.Fatalf("expected service to accept work after drain: %v", err)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("failed to parse url: %v", err)
	}

	return u
}
