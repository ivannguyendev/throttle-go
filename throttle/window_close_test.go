package throttle_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/testutil"
	"github.com/ivannguyendev/throttle-go/throttle"
)

func TestCloseRejectsPendingWaitersAndLaterCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: time.Second})
		ctx := t.Context()
		_, _ = w.Exec(ctx, "k", constFn("first"))
		pending := async(func() (string, error) { return w.Wait(ctx, "k", constFn("second")) })
		synctest.Wait()

		if err := w.Close(); err != nil {
			t.Fatalf("Close = %v", err)
		}

		wantKeyError(t, (<-pending).err, throttle.ErrClosed, "k")
		_, err := w.Exec(ctx, "k", constFn("third"))
		wantKeyError(t, err, throttle.ErrClosed, "k")
		_, err = w.Wait(ctx, "other", constFn("fourth"))
		wantKeyError(t, err, throttle.ErrClosed, "other")
		wantStats(t, w.Stats(), throttle.Stats{})
	})
}

func TestCloseIsIdempotent(t *testing.T) {
	w, err := throttle.New[int]("idem:", throttle.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestCloseNeverClearsAnInjectedEngine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine, _ := throttle.NewMemoryEngine(throttle.MemoryEngineConfig{})
		w := newWindow[int](t, throttle.Config{Window: time.Second, Engine: engine})
		_, _ = w.Exec(t.Context(), "k", constFn(1))

		_ = w.Close()

		if engine.Len() != 1 {
			t.Fatalf("injected engine has %d keys after Close, want 1", engine.Len())
		}
		engine.Clear()
	})
}

func TestShutdownWaitsForARunInFlightWithoutCancellingIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: 30 * time.Millisecond})
		waiter := async(func() (string, error) {
			return w.Wait(t.Context(), "k", func(ctx context.Context, _ throttle.Phase) (string, error) {
				time.Sleep(100 * time.Millisecond)
				return "done", ctx.Err()
			})
		})
		synctest.Wait()
		start := time.Now()

		if err := w.Shutdown(t.Context()); err != nil {
			t.Fatalf("Shutdown = %v", err)
		}

		if waited := time.Since(start); waited != 100*time.Millisecond {
			t.Fatalf("Shutdown returned after %v, want it to wait for the run", waited)
		}
		if res := <-waiter; res.val != "done" || res.err != nil {
			t.Fatalf("covered waiter = %q, %v; want the run's value", res.val, res.err)
		}
	})
}

func TestShutdownReturnsTheContextErrorWhenRunsOutliveIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: 30 * time.Millisecond})
		waiter := async(func() (string, error) {
			return w.Wait(t.Context(), "k", func(context.Context, throttle.Phase) (string, error) {
				time.Sleep(time.Second)
				return "done", nil
			})
		})
		synctest.Wait()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()

		if err := w.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
		}
		<-waiter
	})
}

func TestCloseDuringAHungAcquireFailsTheLeadingCallWithErrClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := &testutil.StubEngine{Respond: func(ctx context.Context, _ testutil.Call) testutil.Reply {
			return testutil.Hang(ctx)
		}}
		w := newWindow[int](t, throttle.Config{Engine: engine})
		leading := async(func() (bool, error) { return w.Exec(t.Context(), "k", constFn(1)) })
		synctest.Wait()

		_ = w.Close()

		res := <-leading
		if res.val {
			t.Fatal("fn ran after Close")
		}
		wantKeyError(t, res.err, throttle.ErrClosed, "k")
	})
}
