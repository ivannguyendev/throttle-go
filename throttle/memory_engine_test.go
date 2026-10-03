package throttle_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ivannguyendev/throttle-go/throttle"
)

func newMemoryEngine(t *testing.T, sweep time.Duration) *throttle.MemoryEngine {
	t.Helper()
	e, err := throttle.NewMemoryEngine(throttle.MemoryEngineConfig{SweepInterval: sweep})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Clear)
	return e
}

func TestMemoryEngineSetGetAndExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemoryEngine(t, 0)
		ctx := t.Context()
		if ok, err := e.Set(ctx, "k", "v", throttle.SetOptions{TTL: 100 * time.Millisecond}); !ok || err != nil {
			t.Fatalf("Set = %v, %v", ok, err)
		}
		if v, found, _ := e.Get(ctx, "k"); !found || v != "v" {
			t.Fatalf("Get = %q, %v", v, found)
		}
		time.Sleep(100 * time.Millisecond)
		if _, found, _ := e.Get(ctx, "k"); found {
			t.Fatal("key still readable at its expiry")
		}
		if e.Len() != 0 {
			t.Fatalf("Len = %d after reading an expired key, want 0", e.Len())
		}
	})
}

func TestMemoryEngineNXWritesOnlyWhenMissingOrExpired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemoryEngine(t, 0)
		ctx := t.Context()
		nx := throttle.SetOptions{TTL: 50 * time.Millisecond, NX: true}
		if ok, _ := e.Set(ctx, "k", "a", nx); !ok {
			t.Fatal("NX on a missing key did not write")
		}
		if ok, _ := e.Set(ctx, "k", "b", nx); ok {
			t.Fatal("NX on a live key wrote")
		}
		time.Sleep(50 * time.Millisecond)
		if ok, _ := e.Set(ctx, "k", "c", nx); !ok {
			t.Fatal("NX on an expired key did not write")
		}
		if ok, _ := e.Set(ctx, "k", "d", throttle.SetOptions{TTL: time.Second}); !ok {
			t.Fatal("a plain Set did not overwrite")
		}
		if v, _, _ := e.Get(ctx, "k"); v != "d" {
			t.Fatalf("Get = %q, want d", v)
		}
	})
}

func TestMemoryEngineNXIsAtomicUnderContention(t *testing.T) {
	e := newMemoryEngine(t, 0)
	var wins atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if ok, _ := e.Set(t.Context(), "k", "v", throttle.SetOptions{TTL: time.Minute, NX: true}); ok {
				wins.Add(1)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d concurrent NX writers won, want exactly 1", wins.Load())
	}
}

func TestMemoryEngineDelAndClear(t *testing.T) {
	e := newMemoryEngine(t, 0)
	ctx := t.Context()
	for _, k := range []string{"a", "b", "c"} {
		_, _ = e.Set(ctx, k, "v", throttle.SetOptions{TTL: time.Minute})
	}
	_ = e.Del(ctx, "a")
	_ = e.Del(ctx, "missing")
	if e.Len() != 2 {
		t.Fatalf("Len = %d after Del, want 2", e.Len())
	}
	e.Clear()
	if e.Len() != 0 {
		t.Fatalf("Len = %d after Clear, want 0", e.Len())
	}
	if ok, _ := e.Set(ctx, "d", "v", throttle.SetOptions{TTL: time.Minute}); !ok || e.Len() != 1 {
		t.Fatal("engine stopped working after Clear")
	}
}

func TestMemoryEngineSweeperPurgesExpiredKeysThenStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newMemoryEngine(t, time.Second)
		ctx := t.Context()
		_, _ = e.Set(ctx, "short", "v", throttle.SetOptions{TTL: 100 * time.Millisecond})
		_, _ = e.Set(ctx, "long", "v", throttle.SetOptions{TTL: 1500 * time.Millisecond})

		time.Sleep(time.Second)
		synctest.Wait()
		if e.Len() != 1 {
			t.Fatalf("Len = %d after the first sweep, want 1", e.Len())
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if e.Len() != 0 {
			t.Fatalf("Len = %d after the second sweep, want 0", e.Len())
		}
		// The sweeper stopped: the bubble would otherwise never run out of
		// timers, and a new key must start it again.
		_, _ = e.Set(ctx, "again", "v", throttle.SetOptions{TTL: 100 * time.Millisecond})
		time.Sleep(time.Second)
		synctest.Wait()
		if e.Len() != 0 {
			t.Fatalf("Len = %d, want the restarted sweeper to purge the key", e.Len())
		}
	})
}

func TestMemoryEngineRejectsANonPositiveTTL(t *testing.T) {
	e := newMemoryEngine(t, 0)
	if _, err := e.Set(t.Context(), "k", "v", throttle.SetOptions{}); err == nil {
		t.Fatal("Set with TTL 0 succeeded")
	}
}
