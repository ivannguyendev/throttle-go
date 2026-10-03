package throttle

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/shard"
)

// Window is a keyed leading + trailing throttle. The first call for an idle
// key runs at once (Leading); calls made while the key runs or while its
// window is open collapse into exactly one Trailing run at the end of the
// window. Each window starts when a run finishes, so runs of one key never
// overlap, even across instances that share a Redis engine.
//
// A Window is safe for concurrent use. Create it with New and release it
// with Close or Shutdown. T is the result type of the throttled Func; use
// struct{} when only Exec is needed.
type Window[T any] struct {
	prefix    string
	window    time.Duration
	lease     leaseTiming
	useLease  bool
	guard     *engineGuard
	owned     *MemoryEngine
	logger    *slog.Logger
	engineCtx context.Context
	cancel    context.CancelFunc
	idx       shard.Index
	closed    atomic.Bool
	inflight  sync.WaitGroup
	stats     counters
	sizeWarn  atomic.Int64
	shards    [shard.Count]stateShard[T]
}

// New creates a Window whose engine keys are keyPrefix + key. It starts no
// goroutine; timers are created on demand.
func New[T any](keyPrefix string, cfg Config) (*Window[T], error) {
	rc, err := resolveConfig(keyPrefix, cfg)
	if err != nil {
		return nil, err
	}
	engineCtx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cancel is owned by the Window and called by Close
	w := &Window[T]{
		prefix: keyPrefix,
		window: rc.window,
		lease:  computeLeaseTiming(rc.window),
		// Nobody else can see an owned MemoryEngine, so the local busy
		// state already prevents overlap and the lease would be pure cost.
		useLease:  rc.ownedEngine == nil,
		guard:     newEngineGuard(rc.engine, rc.logger),
		owned:     rc.ownedEngine,
		logger:    rc.logger,
		engineCtx: engineCtx,
		cancel:    cancel,
		idx:       shard.New(),
	}
	for i := range w.shards {
		w.shards[i].m = make(map[string]*keyState[T])
	}
	return w, nil
}

// Stats returns a snapshot of the Window's counters without locking.
func (w *Window[T]) Stats() Stats {
	return w.stats.snapshot()
}

// Close stops every timer and rejects calls that have not run yet with
// ErrClosed. It clears the default MemoryEngine but never an Engine passed
// in Config. It does not cancel runs already executing fn and does not wait
// for them; use Shutdown for that. Exec and Wait after Close return
// ErrClosed. Close is idempotent and always returns nil.
func (w *Window[T]) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	w.cancel()
	for i := range w.shards {
		sh := &w.shards[i]
		sh.mu.Lock()
		for _, s := range sh.m {
			w.cleanup(s, ErrClosed)
		}
		sh.mu.Unlock()
	}
	if w.owned != nil {
		w.owned.Clear()
	}
	return nil
}

// Shutdown closes the Window, then waits until every run started by the
// Window itself (trailing runs and leading runs of Wait) has finished, or
// ctx ends. Leading runs of Exec execute on their caller's goroutine and are
// owned by the caller. When ctx ends first, one helper goroutine keeps
// waiting until those runs return; it holds nothing else.
func (w *Window[T]) Shutdown(ctx context.Context) error {
	_ = w.Close()
	done := make(chan struct{})
	go func() {
		w.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
