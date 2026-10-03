package throttle_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"go.uber.org/goleak"

	"github.com/ivannguyendev/throttle-go/throttle"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newWindow creates a Window that is closed when the test ends. Inside a
// synctest bubble the cleanup runs inside the bubble too.
func newWindow[T any](t *testing.T, cfg throttle.Config) *throttle.Window[T] {
	t.Helper()
	w, err := throttle.New[T]("test:", cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func constFn[T any](v T) throttle.Func[T] {
	return func(context.Context, throttle.Phase) (T, error) { return v, nil }
}

type run struct {
	label string
	phase throttle.Phase
}

// recorder records every run in order.
type recorder struct {
	mu   sync.Mutex
	runs []run
}

func (r *recorder) job(label string) throttle.Func[string] {
	return func(_ context.Context, phase throttle.Phase) (string, error) {
		r.mu.Lock()
		r.runs = append(r.runs, run{label, phase})
		r.mu.Unlock()
		return label + ":" + phase.String(), nil
	}
}

func (r *recorder) all() []run {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.runs)
}

func wantRuns(t *testing.T, r *recorder, want ...run) {
	t.Helper()
	if got := r.all(); !slices.Equal(got, want) {
		t.Fatalf("runs = %v, want %v", got, want)
	}
}

func wantKeyError(t *testing.T, err, target error, key string) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("err = %v, want %v", err, target)
	}
	ke, ok := errors.AsType[*throttle.KeyError](err)
	if !ok || ke.Key != key {
		t.Fatalf("err = %#v, want *KeyError for key %q", err, key)
	}
}

func wantStats(t *testing.T, got, want throttle.Stats) {
	t.Helper()
	if got != want {
		t.Fatalf("Stats() = %+v, want %+v", got, want)
	}
}

// result carries a value and error across a goroutine boundary.
type result[T any] struct {
	val T
	err error
}

// async runs f on a new goroutine and returns its result channel.
func async[T any](f func() (T, error)) <-chan result[T] {
	ch := make(chan result[T], 1)
	go func() {
		v, err := f()
		ch <- result[T]{v, err}
	}()
	return ch
}
