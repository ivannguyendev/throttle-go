package throttle

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/shard"
)

const defaultSweepInterval = time.Minute

// MemoryEngineConfig tunes a MemoryEngine. The zero value uses defaults.
type MemoryEngineConfig struct {
	// SweepInterval is how often expired keys are purged while any key
	// exists. Default 1m.
	SweepInterval time.Duration
}

// MemoryEngine is an in-process Engine for a single process. Expiry uses
// Go's monotonic clock, so wall-clock jumps don't affect it. Keys are split
// across lock shards, and a lazy sweeper purges expired keys only while keys
// exist, so an idle engine owns no timer. It is safe for concurrent use and
// can back several Windows.
type MemoryEngine struct {
	idx           shard.Index
	shards        [shard.Count]memoryShard
	size          atomic.Int64
	sweepInterval time.Duration

	sweepMu sync.Mutex
	sweeper *time.Timer
	// sweeping is written under sweepMu and read lock-free by Set.
	sweeping atomic.Bool
}

type memoryShard struct {
	mu sync.Mutex
	m  map[string]memoryEntry
	_  [48]byte // keep each shard's lock on its own cache line
}

type memoryEntry struct {
	value   string
	expires time.Time
}

// NewMemoryEngine returns an empty engine.
func NewMemoryEngine(cfg MemoryEngineConfig) (*MemoryEngine, error) {
	if cfg.SweepInterval < 0 {
		return nil, invalidArgument("SweepInterval must not be negative")
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = defaultSweepInterval
	}
	e := &MemoryEngine{idx: shard.New(), sweepInterval: cfg.SweepInterval}
	for i := range e.shards {
		e.shards[i].m = make(map[string]memoryEntry)
	}
	return e, nil
}

// Len returns the number of stored keys, including expired keys that have
// not been purged yet.
func (e *MemoryEngine) Len() int { return int(e.size.Load()) }

// Get implements Engine. It fails with ctx.Err() once ctx has ended.
func (e *MemoryEngine) Get(ctx context.Context, key string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	now := time.Now()
	s := &e.shards[e.idx.Of(key)]
	s.mu.Lock()
	entry, ok := s.m[key]
	if ok && !now.Before(entry.expires) {
		delete(s.m, key)
		e.size.Add(-1)
		ok = false
	}
	s.mu.Unlock()
	return entry.value, ok, nil
}

// Set implements Engine. It fails with ctx.Err() once ctx has ended, so an
// acquire cut short by Window.Close never leaves a key behind.
func (e *MemoryEngine) Set(ctx context.Context, key, value string, opt SetOptions) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if opt.TTL <= 0 {
		return false, invalidArgument("TTL must be positive")
	}
	now := time.Now()
	s := &e.shards[e.idx.Of(key)]
	s.mu.Lock()
	old, existed := s.m[key]
	if opt.NX && existed && now.Before(old.expires) {
		s.mu.Unlock()
		return false, nil
	}
	s.m[key] = memoryEntry{value: value, expires: now.Add(opt.TTL)}
	if !existed {
		e.size.Add(1)
	}
	s.mu.Unlock()
	// Checked after every insert, not only on the 0→1 transition: a Clear
	// running concurrently may have stopped the sweeper while keys remain.
	if !existed && !e.sweeping.Load() {
		e.startSweeper()
	}
	return true, nil
}

// Del implements Engine.
func (e *MemoryEngine) Del(_ context.Context, key string) error {
	s := &e.shards[e.idx.Of(key)]
	s.mu.Lock()
	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		e.size.Add(-1)
	}
	s.mu.Unlock()
	return nil
}

// Clear removes every key and stops the sweeper. The engine keeps working.
// The sweeper stops first, so a key set during Clear starts a new one.
func (e *MemoryEngine) Clear() {
	e.sweepMu.Lock()
	if e.sweeper != nil {
		e.sweeper.Stop()
	}
	e.sweeping.Store(false)
	e.sweepMu.Unlock()
	for i := range e.shards {
		s := &e.shards[i]
		s.mu.Lock()
		e.size.Add(-int64(len(s.m)))
		clear(s.m)
		s.mu.Unlock()
	}
}

func (e *MemoryEngine) startSweeper() {
	e.sweepMu.Lock()
	defer e.sweepMu.Unlock()
	if e.sweeping.Load() {
		return
	}
	e.sweeping.Store(true)
	if e.sweeper == nil {
		e.sweeper = time.AfterFunc(e.sweepInterval, e.sweep)
		return
	}
	e.sweeper.Reset(e.sweepInterval)
}

// sweep purges expired keys, then re-arms itself only while keys remain.
// It stops under sweepMu after reading size; a key inserted after that read
// sees sweeping == false in Set and starts a new sweeper.
func (e *MemoryEngine) sweep() {
	now := time.Now()
	for i := range e.shards {
		s := &e.shards[i]
		s.mu.Lock()
		for key, entry := range s.m {
			if !now.Before(entry.expires) {
				delete(s.m, key)
				e.size.Add(-1)
			}
		}
		s.mu.Unlock()
	}
	e.sweepMu.Lock()
	defer e.sweepMu.Unlock()
	if !e.sweeping.Load() {
		return
	}
	if e.size.Load() > 0 {
		e.sweeper.Reset(e.sweepInterval)
		return
	}
	e.sweeping.Store(false)
}
