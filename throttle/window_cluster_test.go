package throttle_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/testutil"
	"github.com/ivannguyendev/throttle-go/throttle"
)

func TestNoOtherInstanceCanAcquireTheWindowDuringALongRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine, _ := throttle.NewMemoryEngine(throttle.MemoryEngineConfig{})
		w := newWindow[struct{}](t, throttle.Config{Window: 50 * time.Millisecond, Engine: engine})
		var running atomic.Bool
		running.Store(true)

		leading := async(func() (bool, error) {
			return w.Exec(t.Context(), "k", func(context.Context, throttle.Phase) (struct{}, error) {
				time.Sleep(300 * time.Millisecond)
				running.Store(false)
				return struct{}{}, nil
			})
		})
		synctest.Wait()
		polls, wins := 0, 0
		for running.Load() {
			polls++
			if ok, _ := engine.Set(t.Context(), "test:k", "1", throttle.SetOptions{TTL: 50 * time.Millisecond, NX: true}); ok {
				wins++
			}
			time.Sleep(10 * time.Millisecond)
		}

		if res := <-leading; !res.val {
			t.Fatal("leading run did not run")
		}
		if wins != 0 || polls < 15 {
			t.Fatalf("other instance won %d of %d polls", wins, polls)
		}
		engine.Clear()
	})
}

func TestTheLeaseNeverHasMoreThanOneExtendInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := &testutil.StubEngine{Respond: func(ctx context.Context, call testutil.Call) testutil.Reply {
			switch call.Op {
			case testutil.OpExtend:
				return testutil.Hang(ctx)
			case testutil.OpAcquire:
				return testutil.Reply{OK: true}
			default:
				return testutil.Reply{}
			}
		}}
		w := newWindow[struct{}](t, throttle.Config{Window: 50 * time.Millisecond, Engine: engine})
		extends := make(chan []testutil.Call, 1)

		leading := async(func() (bool, error) {
			return w.Exec(t.Context(), "k", func(context.Context, throttle.Phase) (struct{}, error) {
				time.Sleep(350 * time.Millisecond)
				var calls []testutil.Call
				for _, c := range engine.Calls() {
					if c.Op == testutil.OpExtend {
						calls = append(calls, c)
					}
				}
				extends <- calls
				return struct{}{}, nil
			})
		})

		got := <-extends
		if len(got) != 1 || got[0].Key != "test:k" || got[0].Opt.TTL != 200*time.Millisecond {
			t.Fatalf("extends during the run = %+v, want one extend of test:k with TTL 200ms", got)
		}
		_ = w.Close()
		<-leading
	})
}

// Two instances share one engine: each key runs at most once per window
// across both, and a run never starts before the previous run's window ends.
func TestInstancesSharingAnEngineNeverOverlapAndRespectTheWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const window = 50 * time.Millisecond
		engine, _ := throttle.NewMemoryEngine(throttle.MemoryEngineConfig{})
		a := newWindow[struct{}](t, throttle.Config{Window: window, Engine: engine})
		b := newWindow[struct{}](t, throttle.Config{Window: window, Engine: engine})
		var mu sync.Mutex
		type span struct{ start, end time.Time }
		var spans []span
		job := func(context.Context, throttle.Phase) (struct{}, error) {
			start := time.Now()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			spans = append(spans, span{start, time.Now()})
			mu.Unlock()
			return struct{}{}, nil
		}

		var wg sync.WaitGroup
		for i := range 40 {
			w := a
			if i%2 == 1 {
				w = b
			}
			wg.Go(func() { _, _ = w.Exec(t.Context(), "k", job) })
			time.Sleep(5 * time.Millisecond)
		}
		wg.Wait()
		time.Sleep(time.Second)

		mu.Lock()
		defer mu.Unlock()
		slices.SortFunc(spans, func(x, y span) int { return x.start.Compare(y.start) })
		if len(spans) < 2 {
			t.Fatalf("only %d runs", len(spans))
		}
		for i := 1; i < len(spans); i++ {
			if gap := spans[i].start.Sub(spans[i-1].end); gap < window {
				t.Fatalf("run %d started %v after run %d ended, want >= %v", i, gap, i-1, window)
			}
		}
		engine.Clear()
	})
}
