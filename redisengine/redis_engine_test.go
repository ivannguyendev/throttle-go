package redisengine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"

	"github.com/ivannguyendev/throttle-go/redisengine"
	"github.com/ivannguyendev/throttle-go/throttle"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newMiniredisEngine(t *testing.T) (*redisengine.Engine, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	engine, err := redisengine.New(client)
	if err != nil {
		t.Fatal(err)
	}
	return engine, server
}

func TestNewRequiresAClient(t *testing.T) {
	if _, err := redisengine.New(nil); err == nil {
		t.Fatal("New(nil) succeeded")
	}
}

func TestGetOfAMissingKeyIsNotFoundAndNotAnError(t *testing.T) {
	engine, _ := newMiniredisEngine(t)
	value, found, err := engine.Get(t.Context(), "missing")
	if value != "" || found || err != nil {
		t.Fatalf("Get = %q, %v, %v; want not found, nil", value, found, err)
	}
}

func TestSetNXWritesOnceAndALostRaceIsNotAnError(t *testing.T) {
	engine, server := newMiniredisEngine(t)
	ctx := t.Context()
	nx := throttle.SetOptions{TTL: 1500 * time.Millisecond, NX: true}

	if ok, err := engine.Set(ctx, "k", "a", nx); !ok || err != nil {
		t.Fatalf("first NX = %v, %v", ok, err)
	}
	if ok, err := engine.Set(ctx, "k", "b", nx); ok || err != nil {
		t.Fatalf("second NX = %v, %v; want false, nil", ok, err)
	}
	if got := server.TTL("k"); got != 1500*time.Millisecond {
		t.Fatalf("TTL = %v, want 1.5s", got)
	}
	server.FastForward(1500 * time.Millisecond)
	if ok, _ := engine.Set(ctx, "k", "c", nx); !ok {
		t.Fatal("NX on an expired key did not write")
	}
}

func TestSetWithoutNXOverwritesAndGetReadsIt(t *testing.T) {
	engine, server := newMiniredisEngine(t)
	ctx := t.Context()
	_, _ = engine.Set(ctx, "k", "a", throttle.SetOptions{TTL: time.Second, NX: true})

	if ok, err := engine.Set(ctx, "k", "b", throttle.SetOptions{TTL: 200 * time.Millisecond}); !ok || err != nil {
		t.Fatalf("Set = %v, %v", ok, err)
	}
	if value, found, _ := engine.Get(ctx, "k"); value != "b" || !found {
		t.Fatalf("Get = %q, %v", value, found)
	}
	if got := server.TTL("k"); got != 200*time.Millisecond {
		t.Fatalf("TTL = %v, want 200ms", got)
	}
}

func TestSubMillisecondTTLIsRoundedUpNeverToNoExpiry(t *testing.T) {
	engine, server := newMiniredisEngine(t)
	_, _ = engine.Set(t.Context(), "k", "v", throttle.SetOptions{TTL: time.Microsecond})
	if got := server.TTL("k"); got != time.Millisecond {
		t.Fatalf("TTL = %v, want 1ms", got)
	}
}

func TestDel(t *testing.T) {
	engine, server := newMiniredisEngine(t)
	_, _ = engine.Set(t.Context(), "k", "v", throttle.SetOptions{TTL: time.Second})
	if err := engine.Del(t.Context(), "k"); err != nil {
		t.Fatal(err)
	}
	if server.Exists("k") {
		t.Fatal("key still exists after Del")
	}
}

func TestServerErrorsAreReturned(t *testing.T) {
	engine, server := newMiniredisEngine(t)
	server.SetError("LOADING")
	ctx := t.Context()
	if _, _, err := engine.Get(ctx, "k"); err == nil {
		t.Fatal("Get swallowed a server error")
	}
	if _, err := engine.Set(ctx, "k", "v", throttle.SetOptions{TTL: time.Second, NX: true}); err == nil {
		t.Fatal("Set swallowed a server error")
	}
}

func TestTwoWindowsOnOneRedisShareTheWindow(t *testing.T) {
	engine, _ := newMiniredisEngine(t)
	ctx := t.Context()
	newWindow := func() *throttle.Window[int] {
		w, err := throttle.New[int]("sync:", throttle.Config{Window: time.Minute, Engine: engine})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Close() })
		return w
	}
	a, b := newWindow(), newWindow()
	fn := func(context.Context, throttle.Phase) (int, error) { return 1, nil }

	if ran, err := a.Exec(ctx, "room", fn); !ran || err != nil {
		t.Fatalf("instance a = %v, %v; want the leading run", ran, err)
	}
	if ran, err := b.Exec(ctx, "room", fn); ran || err != nil {
		t.Fatalf("instance b = %v, %v; want it to lose the window race", ran, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := b.Wait(waitCtx, "room", fn); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait on instance b = %v, want it to wait for the remote window", err)
	}
}
