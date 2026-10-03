package throttle

import (
	"context"
	"log/slog"
	"time"
)

const (
	sizeWarnThreshold = 5000
	sizeWarnInterval  = time.Minute
)

// Every helper checks Enabled first, so a silent Window (the default) pays
// nothing for logging: no attribute is built and nothing is boxed.

func (w *Window[T]) logRun(ctx context.Context, key string, phase Phase) {
	if w.logger.Enabled(ctx, slog.LevelInfo) {
		w.logger.LogAttrs(ctx, slog.LevelInfo, "throttle run",
			slog.String("key", key), slog.String("phase", phase.String()))
	}
}

func (w *Window[T]) logRunFailed(ctx context.Context, key string, phase Phase, err error) {
	if w.logger.Enabled(ctx, slog.LevelError) {
		w.logger.LogAttrs(ctx, slog.LevelError, "throttle run failed",
			slog.String("key", key), slog.String("phase", phase.String()),
			slog.String("error", err.Error()))
	}
}

func (w *Window[T]) logRearmLimit(key string, rearms int) {
	ctx := context.Background()
	if w.logger.Enabled(ctx, slog.LevelDebug) {
		w.logger.LogAttrs(ctx, slog.LevelDebug, "throttle rearm limit exceeded",
			slog.String("key", key), slog.Int("rearms", rearms))
	}
}

// warnKeyCount logs an unusually large key map at most once per interval.
func (w *Window[T]) warnKeyCount(keys int64) {
	ctx := context.Background()
	if !w.logger.Enabled(ctx, slog.LevelDebug) {
		return
	}
	now := time.Now().UnixNano()
	last := w.sizeWarn.Load()
	if last != 0 && now-last < int64(sizeWarnInterval) {
		return
	}
	if w.sizeWarn.CompareAndSwap(last, now) {
		w.logger.LogAttrs(ctx, slog.LevelDebug, "throttle key map is unusually large",
			slog.Int64("keys", keys))
	}
}
