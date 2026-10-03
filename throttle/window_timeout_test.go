package throttle_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/testutil"
	"github.com/ivannguyendev/throttle-go/throttle"
)

func TestWaitDeadlineReturnsInTimeWhileFnStillCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: 30 * time.Millisecond})
		var completed atomic.Bool
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()

		_, err := w.Wait(ctx, "k", func(context.Context, throttle.Phase) (string, error) {
			time.Sleep(100 * time.Millisecond)
			completed.Store(true)
			return "late", nil
		})

		wantKeyError(t, err, context.DeadlineExceeded, "k")
		if completed.Load() {
			t.Fatal("fn completed before the deadline")
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if !completed.Load() {
			t.Fatal("fn was cancelled by the waiter's deadline")
		}
	})
}

func TestAWaiterThatFinishesInTimeIsNotAffected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: 30 * time.Millisecond})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		got, err := w.Wait(ctx, "k", func(_ context.Context, p throttle.Phase) (string, error) {
			return p.String(), nil
		})

		if got != "leading" || err != nil {
			t.Fatalf("Wait = %q, %v; want leading, nil", got, err)
		}
	})
}

func TestAHungEngineKeepsTheKeyBusyButTheDeadlineSettlesTheCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := &testutil.StubEngine{Respond: func(ctx context.Context, _ testutil.Call) testutil.Reply {
			return testutil.Hang(ctx)
		}}
		w := newWindow[struct{}](t, throttle.Config{Window: 30 * time.Millisecond, Engine: engine})
		var runs atomic.Int64
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		_, err := w.Wait(ctx, "k", func(context.Context, throttle.Phase) (struct{}, error) {
			runs.Add(1)
			return struct{}{}, nil
		})

		wantKeyError(t, err, context.DeadlineExceeded, "k")
		if runs.Load() != 0 {
			t.Fatal("fn ran although the engine never answered")
		}
		if got := w.Stats().Active; got != 1 {
			t.Fatalf("Active = %d, want 1", got)
		}
		if ran, _ := w.Exec(t.Context(), "k", constFn(struct{}{})); ran {
			t.Fatal("a call on the stalled key did not coalesce")
		}
	})
}

func TestAWindowThatCanNeverBeAcquiredIsDroppedAfterTenRearms(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := &testutil.StubEngine{Respond: func(context.Context, testutil.Call) testutil.Reply {
			return testutil.Reply{}
		}}
		logger, records := testutil.NewRecordingLogger()
		w := newWindow[struct{}](t, throttle.Config{Window: 30 * time.Millisecond, Engine: engine, Logger: logger})
		var runs atomic.Int64

		_, err := w.Wait(t.Context(), "k", func(context.Context, throttle.Phase) (struct{}, error) {
			runs.Add(1)
			return struct{}{}, nil
		})

		wantKeyError(t, err, throttle.ErrDropped, "k")
		synctest.Wait()
		if runs.Load() != 0 {
			t.Fatal("fn ran without acquiring the window")
		}
		wantStats(t, w.Stats(), throttle.Stats{})
		if got := engine.Count(testutil.OpAcquire); got != 12 {
			t.Fatalf("acquire calls = %d, want 12 (1 leading + 11 trailing)", got)
		}
		drops := records.Find("throttle rearm limit exceeded")
		if len(drops) != 1 || drops[0].Attrs["rearms"] != "11" || drops[0].Attrs["key"] != "k" {
			t.Fatalf("rearm limit records = %+v", drops)
		}
	})
}
