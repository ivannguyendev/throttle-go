package throttle

import (
	"log/slog"
	"strings"
	"time"
)

const defaultWindow = time.Second

// Config tunes a Window. The zero value is valid and uses the defaults.
type Config struct {
	// Window is the window length, measured from the end of each run.
	// Default 1s; minimum 1ms. Every instance that shares windows through
	// one engine must use the same key prefix and Window.
	Window time.Duration
	// Engine is the window store. nil creates a MemoryEngine owned by the
	// Window and cleared on Close. An Engine you pass in is never cleared or
	// closed by the Window.
	Engine Engine
	// Name tags log records ("throttle" attribute). Default: the key prefix
	// without a trailing ':'.
	Name string
	// Logger receives every run (Info), failed trailing runs and panics
	// (Error), and the breaker opening, the rearm limit and an unusually
	// large key map (Debug). nil logs nothing, not even errors.
	Logger *slog.Logger
}

// resolvedConfig is Config after validation and defaults.
type resolvedConfig struct {
	window      time.Duration
	engine      Engine
	ownedEngine *MemoryEngine
	logger      *slog.Logger
}

func resolveConfig(keyPrefix string, cfg Config) (resolvedConfig, error) {
	if keyPrefix == "" {
		return resolvedConfig{}, invalidArgument("keyPrefix must be a non-empty string")
	}
	window := cfg.Window
	if window == 0 {
		window = defaultWindow
	}
	if window < time.Millisecond {
		return resolvedConfig{}, invalidArgument("Window must be at least 1ms, got " + window.String())
	}
	name := cfg.Name
	if name == "" {
		name = strings.TrimSuffix(keyPrefix, ":")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	resolved := resolvedConfig{
		window: window,
		engine: cfg.Engine,
		logger: logger.With(slog.String("throttle", name)),
	}
	if resolved.engine == nil {
		owned, err := NewMemoryEngine(MemoryEngineConfig{})
		if err != nil {
			return resolvedConfig{}, err
		}
		resolved.engine = owned
		resolved.ownedEngine = owned
	}
	return resolved, nil
}
