package syncutil_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dstotijn/hetty/pkg/syncutil"
)

func TestInFlightBeginDone(t *testing.T) {
	t.Parallel()

	tracker := syncutil.NewInFlight()

	if !tracker.Begin() {
		t.Fatal("expected Begin to succeed when not draining")
	}

	done := make(chan struct{})

	go func() {
		tracker.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Wait returned while an operation was in flight")
	case <-time.After(20 * time.Millisecond):
	}

	tracker.Done()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after Done")
	}
}

func TestInFlightDrainRejectsNewWork(t *testing.T) {
	t.Parallel()

	tracker := syncutil.NewInFlight()

	if !tracker.Begin() {
		t.Fatal("expected Begin to succeed before draining")
	}

	tracker.BeginDrain()

	if tracker.Begin() {
		t.Fatal("expected Begin to fail while draining")
	}

	done := make(chan struct{})

	go func() {
		tracker.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Wait returned while an operation was in flight")
	case <-time.After(20 * time.Millisecond):
	}

	tracker.Done()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after Done")
	}

	// The tracker can be reused.
	if !tracker.Begin() {
		t.Fatal("expected Begin to succeed after a completed drain")
	}

	tracker.Done()
}

func TestInFlightWaitForTimeout(t *testing.T) {
	t.Parallel()

	tracker := syncutil.NewInFlight()

	if !tracker.Begin() {
		t.Fatal("expected Begin to succeed")
	}

	tracker.BeginDrain()

	stop := make(chan struct{})
	finished := make(chan bool, 1)

	go func() {
		finished <- tracker.WaitFor(stop)
	}()

	close(stop)

	select {
	case ok := <-finished:
		if ok {
			t.Fatal("expected WaitFor to return false when stop is closed")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitFor did not return after stop was closed")
	}

	// Still draining: new work is rejected.
	if tracker.Begin() {
		t.Fatal("expected Begin to fail while drain continues")
	}

	// Finishing the pending work allows the drain to complete.
	finished2 := make(chan bool, 1)

	go func() {
		finished2 <- tracker.WaitFor(make(chan struct{}))
	}()

	select {
	case <-finished2:
		t.Fatal("WaitFor returned while an operation was in flight")
	case <-time.After(20 * time.Millisecond):
	}

	tracker.Done()

	select {
	case ok := <-finished2:
		if !ok {
			t.Fatal("expected WaitFor to return true after pending work finished")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitFor did not return after pending work finished")
	}

	if !tracker.Begin() {
		t.Fatal("expected tracker to accept new work after drain completed")
	}

	tracker.Done()
}

func TestInFlightConcurrentBeginDone(t *testing.T) {
	t.Parallel()

	tracker := syncutil.NewInFlight()

	var wg sync.WaitGroup

	var completed int64

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if tracker.Begin() {
				atomic.AddInt64(&completed, 1)
				tracker.Done()
			}
		}()
	}

	wg.Wait()

	drained := make(chan struct{})

	go func() {
		tracker.BeginDrain()
		tracker.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete after all operations finished")
	}

	if atomic.LoadInt64(&completed) == 0 {
		t.Fatal("expected at least some operations to complete")
	}
}
