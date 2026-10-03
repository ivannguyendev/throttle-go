package throttle_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ivannguyendev/throttle-go/throttle"
)

func TestFirstCallRunsLeadingAndReturnsTrue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: 50 * time.Millisecond})
		var r recorder

		ran, err := w.Exec(t.Context(), "k", r.job("first"))

		if !ran || err != nil {
			t.Fatalf("Exec = %v, %v; want true, nil", ran, err)
		}
		wantRuns(t, &r, run{"first", throttle.Leading})
	})
}

func TestCallsInsideTheWindowCoalesceIntoOneTrailingRunOfTheLatestFn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: 50 * time.Millisecond})
		var r recorder
		ctx := t.Context()
		_, _ = w.Exec(ctx, "k", r.job("first"))

		second, _ := w.Exec(ctx, "k", r.job("second"))
		third, _ := w.Exec(ctx, "k", r.job("third"))
		time.Sleep(120 * time.Millisecond)
		synctest.Wait()

		if second || third {
			t.Fatalf("coalesced Exec = %v, %v; want false, false", second, third)
		}
		wantRuns(t, &r, run{"first", throttle.Leading}, run{"third", throttle.Trailing})
	})
}

func TestTheNextWindowIsMeasuredFromTheEndOfTheRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[struct{}](t, throttle.Config{Window: 50 * time.Millisecond})
		ctx := t.Context()
		leadingEnd := make(chan time.Time, 1)
		trailingStart := make(chan time.Time, 1)

		leading := async(func() (bool, error) {
			return w.Exec(ctx, "k", func(context.Context, throttle.Phase) (struct{}, error) {
				time.Sleep(80 * time.Millisecond)
				leadingEnd <- time.Now()
				return struct{}{}, nil
			})
		})
		time.Sleep(10 * time.Millisecond)
		coalesced, _ := w.Exec(ctx, "k", func(context.Context, throttle.Phase) (struct{}, error) {
			trailingStart <- time.Now()
			return struct{}{}, nil
		})
		if res := <-leading; !res.val || coalesced {
			t.Fatalf("leading = %v, coalesced = %v; want true, false", res.val, coalesced)
		}

		if gap := (<-trailingStart).Sub(<-leadingEnd); gap != 52*time.Millisecond {
			t.Fatalf("trailing started %v after the leading run ended, want window + 2ms slack", gap)
		}
	})
}

func TestAnEmptyWindowClosesAndTheNextCallLeadsAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: 30 * time.Millisecond})
		var r recorder
		_, _ = w.Exec(t.Context(), "k", r.job("first"))
		wantStats(t, w.Stats(), throttle.Stats{Keys: 1, Timers: 1})

		time.Sleep(40 * time.Millisecond)
		synctest.Wait()
		wantStats(t, w.Stats(), throttle.Stats{})

		ran, _ := w.Exec(t.Context(), "k", r.job("second"))
		if !ran {
			t.Fatal("second call did not lead")
		}
		wantRuns(t, &r, run{"first", throttle.Leading}, run{"second", throttle.Leading})
	})
}

func TestStatsCountActivePendingKeysAndTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[struct{}](t, throttle.Config{Window: 30 * time.Millisecond})
		ctx := t.Context()
		release := make(chan struct{})
		leading := async(func() (bool, error) {
			return w.Exec(ctx, "k", func(context.Context, throttle.Phase) (struct{}, error) {
				<-release
				return struct{}{}, nil
			})
		})
		synctest.Wait()
		coalesced, _ := w.Exec(ctx, "k", constFn(struct{}{}))

		wantStats(t, w.Stats(), throttle.Stats{Active: 1, Pending: 1, Keys: 1})
		close(release)
		if res := <-leading; !res.val || coalesced {
			t.Fatalf("leading = %v, coalesced = %v; want true, false", res.val, coalesced)
		}
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		wantStats(t, w.Stats(), throttle.Stats{})
	})
}

func TestKeysAreThrottledIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: time.Second})
		var r recorder
		a, _ := w.Exec(t.Context(), "a", r.job("a"))
		b, _ := w.Exec(t.Context(), "b", r.job("b"))
		if !a || !b {
			t.Fatalf("Exec(a) = %v, Exec(b) = %v; want both to lead", a, b)
		}
		wantStats(t, w.Stats(), throttle.Stats{Keys: 2, Timers: 2})
	})
}
