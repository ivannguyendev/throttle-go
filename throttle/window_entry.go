package throttle

import "context"

// Exec throttles fn for key.
//
//   - Leading (the key is idle): runs fn on the calling goroutine with ctx,
//     waits for it and returns true with fn's error. A panic in fn is
//     re-raised here once the key's state is consistent.
//   - Coalesced (the key is running or inside a window, on any instance):
//     returns false at once. The trailing run covers this call; its outcome
//     is not reported here (use Wait for that).
//
// Exec also returns false when this instance lost the window race to
// another instance; the call is then retried when the remote window ends.
// The coalesced path does not allocate.
func (w *Window[T]) Exec(ctx context.Context, key string, fn Func[T]) (bool, error) {
	if err := validateJob(key, fn); err != nil {
		return false, err
	}
	s, _, claimed, err := w.enter(key, fn, false)
	if err != nil || !claimed {
		return false, err
	}
	return w.attempt(ctx, s, Leading, true)
}

// Wait throttles fn for key like Exec, then waits for the run that covers
// this call and returns its value or error. A run covers every call that
// arrived before it started executing fn. The trailing run uses the latest
// fn for the key, so the value may come from a newer closure than this one.
//
// When ctx ends first, Wait returns a *KeyError wrapping ctx.Err(); the run
// is not cancelled and still covers the call. Errors from the Window itself
// (ErrClosed, ErrDropped, ErrPanicked) are wrapped in *KeyError; fn's own
// errors are returned as they are.
func (w *Window[T]) Wait(ctx context.Context, key string, fn Func[T]) (T, error) {
	var zero T
	if err := validateJob(key, fn); err != nil {
		return zero, err
	}
	s, c, claimed, err := w.enter(key, fn, true)
	if err != nil {
		return zero, err
	}
	if claimed {
		runCtx := context.WithoutCancel(ctx)
		go func() {
			defer w.inflight.Done()
			_, _ = w.attempt(runCtx, s, Leading, false)
		}()
	}
	select {
	case <-c.done:
		return c.val, c.err
	case <-ctx.Done():
		return zero, keyError(key, ctx.Err())
	}
}

// enter is the first critical section: register the call (and its waiter)
// synchronously, then claim the key if it is idle (D10, D12).
func (w *Window[T]) enter(key string, fn Func[T], wait bool) (*keyState[T], *cell[T], bool, error) {
	sh := &w.shards[w.idx.Of(key)]
	sh.mu.Lock()
	if w.closed.Load() {
		sh.mu.Unlock()
		return nil, nil, false, keyError(key, ErrClosed)
	}
	s := sh.m[key]
	var keys int64
	if s == nil {
		s, keys = w.newState(sh, key)
	}
	s.fn = fn
	w.setPending(s, true)
	var c *cell[T]
	if wait {
		if s.next == nil {
			s.next = newCell[T]()
		}
		c = s.next
	}
	claimed := !s.armed && !s.busy
	if claimed {
		w.setBusy(s, true)
		if wait {
			w.inflight.Add(1)
		}
	}
	sh.mu.Unlock()
	if keys > sizeWarnThreshold {
		w.warnKeyCount(keys)
	}
	return s, c, claimed, nil
}

func validateJob[T any](key string, fn Func[T]) error {
	if key == "" {
		return invalidArgument("key must be a non-empty string")
	}
	if fn == nil {
		return invalidArgument("fn must not be nil")
	}
	return nil
}
