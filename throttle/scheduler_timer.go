package throttle

import "context"

// onTimer fires at the end of a window (or of a remote window after a lost
// race). With calls pending it starts the trailing run on this goroutine;
// otherwise the window closes and the key is forgotten.
func (s *keyState[T]) onTimer() {
	w := s.w
	s.sh.mu.Lock()
	if !s.armed || !w.alive(s) {
		s.sh.mu.Unlock()
		return
	}
	s.armed = false
	w.stats.timers.Add(-1)
	if !s.pending && s.next == nil {
		w.cleanup(s, ErrDropped)
		s.sh.mu.Unlock()
		return
	}
	w.setBusy(s, true)
	// Added under the shard lock, before Close can clean this shard, so
	// Shutdown never starts waiting while an Add is still possible.
	w.inflight.Add(1)
	s.sh.mu.Unlock()
	defer w.inflight.Done()
	_, _ = w.attempt(context.Background(), s, Trailing, false)
}

// armForRemaining re-arms after a lost window race, for as long as the
// remote window still runs (capped at the lease TTL). It reports false when
// the key died meanwhile.
func (w *Window[T]) armForRemaining(s *keyState[T]) bool {
	delay := w.guard.remaining(w.engineCtx, s.fullKey, w.window, w.lease.ttl)
	s.sh.mu.Lock()
	defer s.sh.mu.Unlock()
	if !w.alive(s) {
		return false
	}
	w.arm(s, delay, rearmMinDelay)
	w.setBusy(s, false)
	return true
}

// recoverState runs when an attempt ends without handing the key to a run
// or a timer (for example an engine panic): retry later if calls are still
// pending, otherwise forget the key.
func (w *Window[T]) recoverState(s *keyState[T]) {
	s.sh.mu.Lock()
	defer s.sh.mu.Unlock()
	if !w.alive(s) {
		return
	}
	w.setBusy(s, false)
	if s.pending || s.next != nil {
		w.arm(s, w.window, rearmMinDelay)
		return
	}
	w.cleanup(s, ErrDropped)
}
