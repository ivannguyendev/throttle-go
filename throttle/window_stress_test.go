package throttle_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ivannguyendev/throttle-go/throttle"
)

// Real time, real goroutines, run under -race in CI: hammer few keys from
// many goroutines and check the invariants that matter in production.
func TestStressRunsOfOneKeyNeverOverlapAndEveryWaiterSettles(t *testing.T) {
	goroutines, iterations := 32, 300
	if testing.Short() {
		goroutines, iterations = 8, 50
	}
	const keys = 8
	engine, _ := throttle.NewMemoryEngine(throttle.MemoryEngineConfig{})
	defer engine.Clear()
	w, err := throttle.New[int64]("stress:", throttle.Config{Window: 2 * time.Millisecond, Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	var running [keys]atomic.Int64
	var overlaps, runs atomic.Int64
	job := func(i int) throttle.Func[int64] {
		return func(context.Context, throttle.Phase) (int64, error) {
			if running[i].Add(1) > 1 {
				overlaps.Add(1)
			}
			time.Sleep(100 * time.Microsecond)
			running[i].Add(-1)
			return runs.Add(1), nil
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var failures atomic.Int64
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for n := range iterations {
				i := (g + n) % keys
				key := "k" + strconv.Itoa(i)
				var err error
				if n%2 == 0 {
					_, err = w.Exec(ctx, key, job(i))
				} else {
					_, err = w.Wait(ctx, key, job(i))
				}
				if err != nil {
					failures.Add(1)
				}
			}
		})
	}
	wg.Wait()

	if err := w.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if overlaps.Load() != 0 {
		t.Fatalf("%d overlapping runs of one key", overlaps.Load())
	}
	if failures.Load() != 0 {
		t.Fatalf("%d calls failed", failures.Load())
	}
	if runs.Load() == 0 || runs.Load() >= int64(goroutines*iterations) {
		t.Fatalf("runs = %d for %d calls: nothing was coalesced", runs.Load(), goroutines*iterations)
	}
	wantStats(t, w.Stats(), throttle.Stats{})
}
