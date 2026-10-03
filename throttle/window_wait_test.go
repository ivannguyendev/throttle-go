package throttle_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/testutil"
	"github.com/ivannguyendev/throttle-go/throttle"
)

func TestWaitReturnsTheLeadingValueThenTheTrailingValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[string](t, throttle.Config{Window: 30 * time.Millisecond})
		var r recorder

		first, err := w.Wait(t.Context(), "k", r.job("first"))
		if first != "first:leading" || err != nil {
			t.Fatalf("first Wait = %q, %v", first, err)
		}
		second, err := w.Wait(t.Context(), "k", r.job("second"))
		if second != "second:trailing" || err != nil {
			t.Fatalf("second Wait = %q, %v", second, err)
		}
	})
}

func TestWaitersOfOneWindowShareTheValueOfOneRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[int64](t, throttle.Config{Window: 30 * time.Millisecond})
		var runs atomic.Int64
		job := func(context.Context, throttle.Phase) (int64, error) { return runs.Add(1), nil }
		_, _ = w.Exec(t.Context(), "k", job)

		waits := make([]<-chan result[int64], 3)
		for i := range waits {
			waits[i] = async(func() (int64, error) { return w.Wait(t.Context(), "k", job) })
		}
		for i, ch := range waits {
			if res := <-ch; res.val != 2 || res.err != nil {
				t.Fatalf("waiter %d = %d, %v; want the trailing run's value 2", i, res.val, res.err)
			}
		}
		if got := runs.Load(); got != 2 {
			t.Fatalf("runs = %d, want 2", got)
		}
	})
}

func TestACallThatArrivesWhileFnRunsIsCoveredByTheNextRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[int64](t, throttle.Config{Window: 30 * time.Millisecond})
		release := make(chan struct{})
		var runs atomic.Int64
		job := func(context.Context, throttle.Phase) (int64, error) {
			n := runs.Add(1)
			if n == 1 {
				<-release
			}
			return n, nil
		}

		first := async(func() (int64, error) { return w.Wait(t.Context(), "k", job) })
		synctest.Wait()
		second := async(func() (int64, error) { return w.Wait(t.Context(), "k", job) })
		synctest.Wait()
		close(release)

		if res := <-first; res.val != 1 {
			t.Fatalf("first = %d, want 1", res.val)
		}
		if res := <-second; res.val != 2 {
			t.Fatalf("second = %d, want 2", res.val)
		}
	})
}

// gatedEngine answers acquire through gate and everything else at once.
func gatedEngine(gate func(ctx context.Context) testutil.Reply) *testutil.StubEngine {
	return &testutil.StubEngine{Respond: func(ctx context.Context, call testutil.Call) testutil.Reply {
		switch call.Op {
		case testutil.OpAcquire:
			return gate(ctx)
		case testutil.OpGet:
			end := time.Now().Add(30 * time.Millisecond).UnixMilli()
			return testutil.Reply{Value: strconv.FormatInt(end, 10), Found: true}
		default:
			return testutil.Reply{OK: true}
		}
	}}
}

func TestCallsDuringAWonLeadingAcquireAreCoveredByTheLeadingRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan bool)
		engine := gatedEngine(func(ctx context.Context) testutil.Reply {
			return testutil.Reply{OK: <-gate}
		})
		w := newWindow[string](t, throttle.Config{Window: 30 * time.Millisecond, Engine: engine})
		var r recorder

		leader := async(func() (bool, error) { return w.Exec(t.Context(), "k", r.job("leader")) })
		synctest.Wait()
		follower := async(func() (string, error) { return w.Wait(t.Context(), "k", r.job("follower")) })
		synctest.Wait()
		gate <- true

		if res := <-leader; !res.val {
			t.Fatal("leader did not run")
		}
		if res := <-follower; res.val != "follower:leading" {
			t.Fatalf("follower = %q, want the leading run (latest fn) to cover it", res.val)
		}
		time.Sleep(100 * time.Millisecond)
		wantRuns(t, &r, run{"follower", throttle.Leading})
	})
}

func TestCallsDuringALostLeadingAcquireAreCoveredByTheTrailingRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan bool)
		var acquires atomic.Int64
		engine := gatedEngine(func(ctx context.Context) testutil.Reply {
			if acquires.Add(1) == 1 {
				return testutil.Reply{OK: <-gate}
			}
			return testutil.Reply{OK: true}
		})
		w := newWindow[string](t, throttle.Config{Window: 30 * time.Millisecond, Engine: engine})
		var r recorder

		leader := async(func() (bool, error) { return w.Exec(t.Context(), "k", r.job("leader")) })
		synctest.Wait()
		follower := async(func() (string, error) { return w.Wait(t.Context(), "k", r.job("follower")) })
		synctest.Wait()
		gate <- false

		if res := <-leader; res.val || res.err != nil {
			t.Fatalf("leader = %v, %v; want false, nil after losing the race", res.val, res.err)
		}
		if res := <-follower; res.val != "follower:trailing" {
			t.Fatalf("follower = %q, want the trailing run", res.val)
		}
		wantRuns(t, &r, run{"follower", throttle.Trailing})
	})
}
