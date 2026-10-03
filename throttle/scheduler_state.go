package throttle

import (
	"sync"
	"time"
)

// keyState is the in-memory state of one key. Every field is guarded by the
// lock of the shard that holds it (sh.mu). It lives from the first call for
// the key until a window passes with no calls, the key is dropped, or the
// Window closes.
//
// Invariant: armed and busy are never both true. A timer is armed only by
// the flow that holds busy, right before it releases busy, so at most one
// timer callback is ever pending for a key and no generation token is needed.
type keyState[T any] struct {
	w       *Window[T]
	sh      *stateShard[T]
	key     string
	fullKey string
	fn      Func[T]
	// next collects the waiters for the run that has not started yet.
	next    *cell[T]
	timer   *time.Timer
	lease   *lease
	rearms  int
	pending bool
	busy    bool
	armed   bool
	dead    bool
}

type stateShard[T any] struct {
	mu sync.Mutex
	m  map[string]*keyState[T]
	_  [48]byte // keep each shard's lock on its own cache line
}

// cell broadcasts the outcome of one run to every waiter it covers. Waiters
// share one cell, so a waiter costs no allocation, and a waiter that gives up
// on its context leaves nothing behind.
type cell[T any] struct {
	done chan struct{}
	val  T
	err  error
}

func newCell[T any]() *cell[T] {
	return &cell[T]{done: make(chan struct{})}
}

// settle publishes the outcome. The writes happen before close, so every
// waiter that receives from done reads them safely.
func (c *cell[T]) settle(val T, err error) {
	c.val, c.err = val, err
	close(c.done)
}

func (w *Window[T]) newState(sh *stateShard[T], key string) (*keyState[T], int64) {
	s := &keyState[T]{w: w, sh: sh, key: key, fullKey: w.prefix + key}
	sh.m[key] = s
	return s, w.stats.keys.Add(1)
}

// alive reports whether s may still be touched: it was neither cleaned up
// nor is its Window closed. Check it after every blocking call (D13).
func (w *Window[T]) alive(s *keyState[T]) bool {
	return !s.dead && !w.closed.Load()
}

func (w *Window[T]) setBusy(s *keyState[T], busy bool) {
	if s.busy != busy {
		s.busy = busy
		w.stats.active.Add(boolDelta(busy))
	}
}

func (w *Window[T]) setPending(s *keyState[T], pending bool) {
	if s.pending != pending {
		s.pending = pending
		w.stats.pending.Add(boolDelta(pending))
	}
}

// arm schedules the window-end callback, reusing the key's timer.
func (w *Window[T]) arm(s *keyState[T], delay, minDelay time.Duration) {
	if !w.alive(s) {
		return
	}
	delay = max(delay, minDelay)
	if !s.armed {
		s.armed = true
		w.stats.timers.Add(1)
	}
	if s.timer == nil {
		s.timer = time.AfterFunc(delay, s.onTimer)
		return
	}
	s.timer.Reset(delay)
}

// cleanup removes s and rejects its pending waiters with reason. A run that
// is already executing keeps its covered waiters and settles them itself.
func (w *Window[T]) cleanup(s *keyState[T], reason error) {
	if s.dead {
		return
	}
	s.dead = true
	if s.armed {
		s.timer.Stop()
		s.armed = false
		w.stats.timers.Add(-1)
	}
	if l := s.lease; l != nil {
		s.lease = nil
		w.stats.timers.Add(-1)
		l.halt()
	}
	if c := s.next; c != nil {
		s.next = nil
		var zero T
		c.settle(zero, keyError(s.key, reason))
	}
	w.setPending(s, false)
	w.setBusy(s, false)
	delete(s.sh.m, s.key)
	w.stats.keys.Add(-1)
}

func boolDelta(on bool) int64 {
	if on {
		return 1
	}
	return -1
}
