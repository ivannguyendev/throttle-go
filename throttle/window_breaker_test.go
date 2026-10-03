package throttle_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/testutil"
	"github.com/ivannguyendev/throttle-go/throttle"
)

func TestAFailingEngineFailsOpenAndTripsTheBreaker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := &testutil.StubEngine{Respond: func(context.Context, testutil.Call) testutil.Reply {
			return testutil.Reply{Err: errors.New("redis down")}
		}}
		logger, records := testutil.NewRecordingLogger()
		w := newWindow[int](t, throttle.Config{Window: time.Second, Engine: engine, Logger: logger})
		exec := func(key string) {
			t.Helper()
			if ran, err := w.Exec(t.Context(), key, constFn(1)); !ran || err != nil {
				t.Fatalf("Exec(%s) = %v, %v; want the job to run (fail-open)", key, ran, err)
			}
		}

		// acquire + extend per run: the 5th failure (3rd acquire) opens it.
		exec("a")
		exec("b")
		exec("c")
		if got := len(engine.Calls()); got != 5 {
			t.Fatalf("engine calls = %d, want 5", got)
		}
		if len(records.Find("throttle engine breaker open")) != 1 {
			t.Fatal("breaker opening was not logged")
		}

		exec("d")
		if got := len(engine.Calls()); got != 5 {
			t.Fatalf("engine called %d times while the breaker is open", got-5)
		}

		time.Sleep(10 * time.Second)
		exec("e")
		if got := len(engine.Calls()); got != 6 {
			t.Fatalf("engine calls after cooldown = %d, want exactly one probe", got)
		}
	})
}

func TestAPanickingEngineIsTreatedAsAFailingEngine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		engine := &testutil.StubEngine{Respond: func(context.Context, testutil.Call) testutil.Reply {
			panic("driver bug")
		}}
		w := newWindow[int](t, throttle.Config{Window: 30 * time.Millisecond, Engine: engine})
		_, _ = w.Exec(t.Context(), "k", constFn(1))

		got, err := w.Wait(t.Context(), "k", constFn(2))

		if got != 2 || err != nil {
			t.Fatalf("Wait = %d, %v; want the trailing run to fail open", got, err)
		}
	})
}

func TestAnUnusuallyLargeKeyMapIsLoggedOncePerMinute(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, records := testutil.NewRecordingLogger()
		w := newWindow[int](t, throttle.Config{Window: time.Hour, Logger: logger})
		for i := range 6000 {
			_, _ = w.Exec(t.Context(), "k"+strconv.Itoa(i), constFn(i))
		}
		if got := len(records.Find("throttle key map is unusually large")); got != 1 {
			t.Fatalf("%d warnings, want 1 per minute", got)
		}
		time.Sleep(time.Minute)
		_, _ = w.Exec(t.Context(), "one-more", constFn(0))
		if got := len(records.Find("throttle key map is unusually large")); got != 2 {
			t.Fatalf("%d warnings after a minute, want 2", got)
		}
	})
}
