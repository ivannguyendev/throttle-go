package throttle

import (
	"context"
	"sync"
	"time"
)

const leaseMinInterval = 100 * time.Millisecond

// leaseTiming: while fn runs, the window is extended every interval with
// ttl, so a run longer than the window never lets another instance acquire
// the same key.
type leaseTiming struct {
	interval time.Duration
	ttl      time.Duration
}

func computeLeaseTiming(window time.Duration) leaseTiming {
	interval := max(window/2, leaseMinInterval)
	return leaseTiming{interval: interval, ttl: max(window, 2*interval)}
}

// lease re-extends one run's window. It reschedules itself only after an
// extend returns, so at most one extend is ever in flight, and it owns no
// goroutine while idle.
type lease struct {
	guard  *engineGuard
	ctx    context.Context
	key    string
	timing leaseTiming

	mu       sync.Mutex
	stopped  bool
	timer    *time.Timer
	inflight sync.WaitGroup
}

func newLease(ctx context.Context, guard *engineGuard, key string, timing leaseTiming) *lease {
	l := &lease{guard: guard, ctx: ctx, key: key, timing: timing}
	l.mu.Lock()
	l.timer = time.AfterFunc(timing.interval, l.tick)
	l.mu.Unlock()
	return l
}

func (l *lease) tick() {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	// Added under mu while not stopped: stop() sets stopped under mu before
	// it waits, so no Add can race with that Wait.
	l.inflight.Add(1)
	l.mu.Unlock()
	defer l.inflight.Done()
	l.guard.extend(l.ctx, l.key, l.timing.ttl)
	l.mu.Lock()
	if !l.stopped {
		l.timer.Reset(l.timing.interval)
	}
	l.mu.Unlock()
}

// halt stops future extends without waiting. Safe to call under other locks.
func (l *lease) halt() {
	l.mu.Lock()
	l.stopped = true
	l.timer.Stop()
	l.mu.Unlock()
}

// stop halts the lease and waits for an extend still in flight.
func (l *lease) stop() {
	l.halt()
	l.inflight.Wait()
}
