package throttle

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	maxRearms        = 10
	rearmMinDelay    = 10 * time.Millisecond
	windowTimerSlack = 2 * time.Millisecond
)

var errGoexit = errors.New("throttle: run called runtime.Goexit")

// attempt drives both leading and trailing runs: acquire the window, then
// either run with a lease, or re-arm for the remaining remote window. Only a
// leading attempt returns an error; inline (Exec) also returns fn's error.
func (w *Window[T]) attempt(runCtx context.Context, s *keyState[T], phase Phase, inline bool) (ran bool, err error) {
	handedOff := false
	defer func() {
		if !handedOff {
			w.recoverState(s)
		}
	}()

	acquired := w.guard.acquire(w.engineCtx, s.fullKey, w.lease.ttl)
	s.sh.mu.Lock()
	if !w.alive(s) {
		s.sh.mu.Unlock()
		if phase == Leading {
			return false, keyError(s.key, ErrClosed)
		}
		return false, nil
	}
	if acquired {
		handedOff = true
		fn, covered, l := w.beginRun(s)
		s.sh.mu.Unlock()
		return true, w.run(runCtx, s, phase, inline, fn, covered, l)
	}
	if phase == Trailing {
		s.rearms++
		if s.rearms > maxRearms {
			rearms := s.rearms
			w.cleanup(s, ErrDropped)
			s.sh.mu.Unlock()
			w.logRearmLimit(s.key, rearms)
			return false, nil
		}
	}
	s.sh.mu.Unlock()
	handedOff = w.armForRemaining(s)
	if phase == Leading && !handedOff {
		return false, keyError(s.key, ErrClosed)
	}
	return false, nil
}

// beginRun is the second critical section: everything a run claims happens
// under one lock, so Close can never slip in between (lock must be held).
func (w *Window[T]) beginRun(s *keyState[T]) (Func[T], *cell[T], *lease) {
	s.rearms = 0
	covered := s.next
	s.next = nil
	w.setPending(s, false)
	var l *lease
	if w.useLease {
		l = newLease(w.engineCtx, w.guard, s.fullKey, w.lease)
		s.lease = l
		w.stats.timers.Add(1)
	}
	return s.fn, covered, l
}

// run executes fn, settles the covered waiters, then reopens the window.
// A failed trailing run is logged (D22); a panic in an inline leading run is
// re-raised on the caller's goroutine once the key's state is consistent.
func (w *Window[T]) run(ctx context.Context, s *keyState[T], phase Phase, inline bool,
	fn Func[T], covered *cell[T], l *lease,
) error {
	finished := false
	defer func() {
		if !finished {
			var zero T
			settle(covered, zero, keyError(s.key, errGoexit))
			w.endRun(s, l)
		}
	}()
	w.logRun(ctx, s.key, phase)
	val, panicVal, panicked, err := call(ctx, fn, phase)
	if panicked {
		err = keyError(s.key, err)
	}
	settle(covered, val, err)
	covered = nil
	if err != nil && (phase == Trailing || (panicked && !inline)) {
		w.logRunFailed(ctx, s.key, phase, err)
	}
	w.endRun(s, l)
	finished = true
	if panicked && inline {
		panic(panicVal)
	}
	if inline {
		return err
	}
	return nil
}

// endRun stops the lease, waits for an extend still in flight (outside any
// lock, so it cannot land after the closing extend), then reopens the window
// from the end of the run.
func (w *Window[T]) endRun(s *keyState[T], l *lease) {
	if l != nil {
		l.stop()
	}
	s.sh.mu.Lock()
	if l != nil && s.lease == l {
		s.lease = nil
		w.stats.timers.Add(-1)
	}
	alive := w.alive(s)
	s.sh.mu.Unlock()
	if alive {
		w.guard.extend(w.engineCtx, s.fullKey, w.window)
	}
	s.sh.mu.Lock()
	if w.alive(s) {
		w.arm(s, w.window+windowTimerSlack, time.Millisecond)
		w.setBusy(s, false)
	}
	s.sh.mu.Unlock()
}

// call runs fn and turns a panic into ErrPanicked. Error is last by
// convention; panicVal is kept so an inline caller can re-panic.
func call[T any](ctx context.Context, fn Func[T], phase Phase) (val T, panicVal any, panicked bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			panicVal, panicked = r, true
			err = fmt.Errorf("%w: %v", ErrPanicked, r)
		}
	}()
	val, err = fn(ctx, phase)
	return val, nil, false, err
}

func settle[T any](c *cell[T], val T, err error) {
	if c != nil {
		c.settle(val, err)
	}
}
