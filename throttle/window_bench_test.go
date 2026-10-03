package throttle_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ivannguyendev/throttle-go/throttle"
)

func benchFn(context.Context, throttle.Phase) (int, error) { return 0, nil }

func newBenchWindow(b *testing.B, engine throttle.Engine) *throttle.Window[int] {
	b.Helper()
	w, err := throttle.New[int]("bench:", throttle.Config{Window: time.Hour, Engine: engine})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = w.Close() })
	return w
}

// The hot path: a burst on one key while its window is open.
func BenchmarkExecCoalescedSameKey(b *testing.B) {
	w := newBenchWindow(b, nil)
	ctx := context.Background()
	_, _ = w.Exec(ctx, "k", benchFn)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = w.Exec(ctx, "k", benchFn)
		}
	})
}

// Many keys spread over the lock shards, as in a real service.
func BenchmarkExecCoalescedManyKeys(b *testing.B) {
	const keys = 1024
	w := newBenchWindow(b, nil)
	ctx := context.Background()
	names := make([]string, keys)
	for i := range names {
		names[i] = "k" + strconv.Itoa(i)
		_, _ = w.Exec(ctx, names[i], benchFn)
	}
	var next atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := next.Add(1) * 7919
		for pb.Next() {
			i++
			_, _ = w.Exec(ctx, names[i%keys], benchFn)
		}
	})
}

// A leading run of a new key, each iteration on a fresh key.
func BenchmarkExecLeadingNewKey(b *testing.B) {
	w := newBenchWindow(b, nil)
	ctx := context.Background()
	names := make([]string, 0, 1<<16)
	for i := range cap(names) {
		names = append(names, "k"+strconv.Itoa(i))
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if i == len(names) {
			b.StopTimer()
			_ = w.Close()
			w = newBenchWindow(b, nil)
			i = 0
			b.StartTimer()
		}
		_, _ = w.Exec(ctx, names[i], benchFn)
		i++
	}
}

func BenchmarkMemoryEngineSetNX(b *testing.B) {
	e, _ := throttle.NewMemoryEngine(throttle.MemoryEngineConfig{})
	b.Cleanup(e.Clear)
	ctx := context.Background()
	opt := throttle.SetOptions{TTL: time.Hour, NX: true}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = e.Set(ctx, "k", "v", opt)
		}
	})
}
