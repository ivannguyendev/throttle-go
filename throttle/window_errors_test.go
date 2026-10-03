package throttle_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/testutil"
	"github.com/ivannguyendev/throttle-go/throttle"
)

var errBoom = errors.New("boom")

func failing(context.Context, throttle.Phase) (int, error) { return 0, errBoom }

func TestALeadingExecReturnsFnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[int](t, throttle.Config{Window: 30 * time.Millisecond})

		ran, err := w.Exec(t.Context(), "k", failing)

		if !ran || !errors.Is(err, errBoom) {
			t.Fatalf("Exec = %v, %v; want true, boom", ran, err)
		}
		if again, _ := w.Exec(t.Context(), "k", constFn(1)); again {
			t.Fatal("a failed leading run did not open a window")
		}
	})
}

func TestAFailedTrailingRunRejectsItsWaitersAndIsLogged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, records := testutil.NewRecordingLogger()
		w := newWindow[int](t, throttle.Config{Window: 30 * time.Millisecond, Logger: logger})
		_, _ = w.Exec(t.Context(), "k", constFn(1))

		_, err := w.Wait(t.Context(), "k", failing)

		if !errors.Is(err, errBoom) {
			t.Fatalf("Wait = %v, want boom", err)
		}
		synctest.Wait()
		failed := records.Find("throttle run failed")
		if len(failed) != 1 || failed[0].Attrs["phase"] != "trailing" || failed[0].Attrs["throttle"] != "test" {
			t.Fatalf("run failed records = %+v", failed)
		}
	})
}

func TestAPanicInALeadingExecIsReraisedAndTheKeyRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWindow[int](t, throttle.Config{Window: 30 * time.Millisecond})
		ctx := t.Context()

		recovered := func() (r any) {
			defer func() { r = recover() }()
			_, _ = w.Exec(ctx, "k", func(context.Context, throttle.Phase) (int, error) { panic("kaboom") })
			return nil
		}()

		if recovered != "kaboom" {
			t.Fatalf("recovered %v, want the original panic value", recovered)
		}
		wantStats(t, w.Stats(), throttle.Stats{Keys: 1, Timers: 1})
		time.Sleep(50 * time.Millisecond)
		if ran, _ := w.Exec(ctx, "k", constFn(1)); !ran {
			t.Fatal("the key did not lead again after its window")
		}
	})
}

func TestAPanicInATrailingRunBecomesErrPanicked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, records := testutil.NewRecordingLogger()
		w := newWindow[int](t, throttle.Config{Window: 30 * time.Millisecond, Logger: logger})
		_, _ = w.Exec(t.Context(), "k", constFn(1))

		_, err := w.Wait(t.Context(), "k", func(context.Context, throttle.Phase) (int, error) { panic("kaboom") })

		wantKeyError(t, err, throttle.ErrPanicked, "k")
		synctest.Wait()
		if len(records.Find("throttle run failed")) != 1 {
			t.Fatal("the trailing panic was not logged")
		}
	})
}

func TestInvalidArguments(t *testing.T) {
	tests := []struct {
		name string
		call func() error
	}{
		{"empty key prefix", func() error { _, err := throttle.New[int]("", throttle.Config{}); return err }},
		{"window below 1ms", func() error {
			_, err := throttle.New[int]("p:", throttle.Config{Window: time.Microsecond})
			return err
		}},
		{"negative window", func() error { _, err := throttle.New[int]("p:", throttle.Config{Window: -time.Second}); return err }},
		{"negative sweep interval", func() error {
			_, err := throttle.NewMemoryEngine(throttle.MemoryEngineConfig{SweepInterval: -1})
			return err
		}},
		{"empty key in Exec", func() error {
			w, _ := throttle.New[int]("p:", throttle.Config{})
			defer w.Close()
			_, err := w.Exec(context.Background(), "", constFn(1))
			return err
		}},
		{"nil fn in Wait", func() error {
			w, _ := throttle.New[int]("p:", throttle.Config{})
			defer w.Close()
			_, err := w.Wait(context.Background(), "k", nil)
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, throttle.ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}
