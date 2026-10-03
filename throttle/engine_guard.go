package throttle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ivannguyendev/throttle-go/internal/breaker"
)

const (
	breakerThreshold = 5
	breakerCooldown  = 10 * time.Second
)

var errEnginePanicked = errors.New("throttle: engine panicked")

// engineGuard maps the scheduler's intents onto the Engine (acquire → SET NX,
// extend → SET, remaining → GET) behind a circuit breaker. It never returns
// an error: on an engine error or an open breaker it fails open (D18), so the
// job still runs and in-memory state keeps throttling per instance.
type engineGuard struct {
	engine  Engine
	breaker *breaker.Breaker
	logger  *slog.Logger
}

func newEngineGuard(engine Engine, logger *slog.Logger) *engineGuard {
	return &engineGuard{
		engine:  engine,
		breaker: breaker.New(breakerThreshold, breakerCooldown),
		logger:  logger,
	}
}

// acquire claims the window with SET NX. Fail-open result: true.
func (g *engineGuard) acquire(ctx context.Context, key string, ttl time.Duration) bool {
	if g.breaker.Admit() == breaker.Open {
		return true
	}
	ok, err := g.set(ctx, key, encodeWindowEnd(time.Now(), ttl), SetOptions{TTL: ttl, NX: true})
	if err != nil {
		g.record(ctx, "acquire", key, err)
		return true
	}
	g.breaker.RecordSuccess()
	return ok
}

// extend overwrites the window end (lease and closing extend).
func (g *engineGuard) extend(ctx context.Context, key string, ttl time.Duration) {
	if g.breaker.Admit() == breaker.Open {
		return
	}
	_, err := g.set(ctx, key, encodeWindowEnd(time.Now(), ttl), SetOptions{TTL: ttl})
	if err != nil {
		g.record(ctx, "extend", key, err)
		return
	}
	g.breaker.RecordSuccess()
}

// remaining reads how long the remote window still runs, capped at maxWait.
// Fail-open result: fallback.
func (g *engineGuard) remaining(ctx context.Context, key string, fallback, maxWait time.Duration) time.Duration {
	if g.breaker.Admit() == breaker.Open {
		return fallback
	}
	value, found, err := g.get(ctx, key)
	if err != nil {
		g.record(ctx, "remaining", key, err)
		return fallback
	}
	g.breaker.RecordSuccess()
	return remainingWindow(value, found, time.Now(), fallback, maxWait)
}

// set and get turn an engine panic into an error: engine calls also run on
// the Window's own goroutines (timers, leases), where a panic would crash
// the process. Like any engine error it counts towards the breaker.
func (g *engineGuard) set(ctx context.Context, key, value string, opt SetOptions) (ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", errEnginePanicked, r)
		}
	}()
	return g.engine.Set(ctx, key, value, opt)
}

func (g *engineGuard) get(ctx context.Context, key string) (value string, found bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", errEnginePanicked, r)
		}
	}()
	return g.engine.Get(ctx, key)
}

// record counts a failure, unless the Window's context ended: a call cut
// short by Close says nothing about the engine's health.
func (g *engineGuard) record(ctx context.Context, op, key string, err error) {
	if ctx.Err() != nil {
		g.breaker.RecordIgnored()
		return
	}
	if g.breaker.RecordFailure() && g.logger.Enabled(ctx, slog.LevelDebug) {
		g.logger.LogAttrs(ctx, slog.LevelDebug, "throttle engine breaker open",
			slog.String("op", op), slog.String("key", key),
			slog.Duration("cooldown", breakerCooldown), slog.String("error", err.Error()))
	}
}
