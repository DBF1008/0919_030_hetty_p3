package syncutil

import (
	"sync"
	"sync/atomic"
)

// InFlight tracks the number of operations that are currently in flight.
// Unlike sync.WaitGroup, an InFlight tracker can be reused across multiple
// drain cycles: Begin calls that arrive while a drain is in progress are
// rejected, and after a successful Wait the tracker accepts new operations
// again.
type InFlight struct {
	// drainMu is held in shared mode while an operation is registered and in
	// exclusive mode between BeginDrain and (a completed) Wait.
	drainMu sync.RWMutex

	// waitMu guards the generations slice, and serializes Done broadcasts with
	// waiters entering a generation.
	waitMu sync.Mutex

	count    int64
	draining bool

	// generations holds one channel per active Wait/WaitFor caller. A channel
	// is closed when the count drops to zero.
	generations []chan struct{}
}

// NewInFlight returns a new InFlight tracker.
func NewInFlight() *InFlight {
	return &InFlight{}
}

// Begin registers a new in-flight operation. It returns false when a drain is
// in progress, in which case the operation must not run and Done must not be
// called.
func (t *InFlight) Begin() bool {
	t.drainMu.RLock()
	defer t.drainMu.RUnlock()

	if t.draining {
		return false
	}

	atomic.AddInt64(&t.count, 1)

	return true
}

// Done unregisters an in-flight operation previously registered with Begin.
func (t *InFlight) Done() {
	if atomic.AddInt64(&t.count, -1) != 0 {
		return
	}

	t.waitMu.Lock()
	waiters := t.generations
	t.generations = nil
	t.waitMu.Unlock()

	for _, ch := range waiters {
		close(ch)
	}
}

// BeginDrain marks the tracker as draining. After BeginDrain returns, Begin
// rejects new operations. It can be called multiple times for the same drain.
func (t *InFlight) BeginDrain() {
	t.drainMu.Lock()
	defer t.drainMu.Unlock()

	t.draining = true
}

// Wait blocks until all in-flight operations have finished, then clears the
// draining state so the tracker can be reused. BeginDrain must have been
// called before Wait.
func (t *InFlight) Wait() {
	<-t.subscribe()

	t.drainMu.Lock()
	t.draining = false
	t.drainMu.Unlock()
}

// WaitFor is like Wait, but it returns early when stop is closed. It returns
// true when all in-flight operations finished and the draining state was
// cleared, or false when stop was reached first. In the latter case the
// tracker remains draining (rejecting new Begin calls), and Wait or WaitFor
// can be called later to finish the drain.
func (t *InFlight) WaitFor(stop <-chan struct{}) bool {
	ch := t.subscribe()

	select {
	case <-ch:
		t.drainMu.Lock()
		t.draining = false
		t.drainMu.Unlock()

		return true
	case <-stop:
		// Remove the subscription so a later count zero doesn't close a
		// channel nobody is listening on.
		t.waitMu.Lock()
		for i, gen := range t.generations {
			if gen == ch {
				t.generations = append(t.generations[:i], t.generations[i+1:]...)
				break
			}
		}
		t.waitMu.Unlock()

		return false
	}
}

// subscribe returns a channel that is closed when the count reaches zero. If
// the count is already zero, the returned channel is already closed.
func (t *InFlight) subscribe() chan struct{} {
	t.waitMu.Lock()
	defer t.waitMu.Unlock()

	if atomic.LoadInt64(&t.count) == 0 {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	ch := make(chan struct{})
	t.generations = append(t.generations, ch)

	return ch
}
