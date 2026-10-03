package redisengine_test

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ivannguyendev/throttle-go/redisengine"
	"github.com/ivannguyendev/throttle-go/throttle"
)

// Runs against a real Redis when REDIS_ADDR is set (CI starts one):
//
//	docker run --rm -p 6379:6379 redis:7
//	REDIS_ADDR=localhost:6379 go test ./...
func TestIntegrationInstancesNeverRunAKeyTwiceAtOnce(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
	})
	defer client.Close()
	engine, err := redisengine.New(client)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "throttle-go-it:" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":"

	const instances = 4
	windows := make([]*throttle.Window[int64], instances)
	for i := range windows {
		windows[i], err = throttle.New[int64](prefix, throttle.Config{Window: 50 * time.Millisecond, Engine: engine})
		if err != nil {
			t.Fatal(err)
		}
	}
	var running, overlaps, runs atomic.Int64
	fn := func(context.Context, throttle.Phase) (int64, error) {
		if running.Add(1) > 1 {
			overlaps.Add(1)
		}
		time.Sleep(5 * time.Millisecond)
		running.Add(-1)
		return runs.Add(1), nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, w := range windows {
		for range 4 {
			wg.Go(func() {
				for range 50 {
					_, _ = w.Exec(ctx, "room", fn)
					time.Sleep(2 * time.Millisecond)
				}
			})
		}
	}
	wg.Wait()
	for _, w := range windows {
		if err := w.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}

	if overlaps.Load() != 0 {
		t.Fatalf("%d overlapping runs across instances", overlaps.Load())
	}
	if calls := int64(instances * 4 * 50); runs.Load() == 0 || runs.Load() >= calls/4 {
		t.Fatalf("runs = %d for %d calls: the instances did not share the window", runs.Load(), calls)
	}
}
