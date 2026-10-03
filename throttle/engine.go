package throttle

import (
	"context"
	"time"
)

// SetOptions are the Redis-style options of Engine.Set.
type SetOptions struct {
	// TTL is the key's expiry. The Window always passes at least 1ms.
	TTL time.Duration
	// NX writes only if the key is missing or expired, atomically.
	NX bool
}

// Engine is the window store: a thin, Redis-style key-value store with
// expiry. Keys arrive already prefixed. All throttle logic lives in the
// Window; an Engine never retries and never applies its own breaker.
//
// Return an error on failure: the Window counts it towards its circuit
// breaker and fails open (the job still runs). There is no timeout around
// engine calls, so configure one on your client. Implementations must be
// safe for concurrent use.
type Engine interface {
	// Get returns the stored value, or found=false when the key is missing
	// or expired.
	Get(ctx context.Context, key string) (value string, found bool, err error)
	// Set stores value with opt.TTL expiry. With opt.NX it writes atomically
	// and only if the key is missing or expired, and reports whether this
	// call wrote it. Without NX it always overwrites (SET ... PX semantics)
	// and reports true. The atomic NX is what stops two instances from
	// running the same key.
	Set(ctx context.Context, key, value string, opt SetOptions) (bool, error)
	// Del removes the key. The Window never calls it; it is there for you.
	Del(ctx context.Context, key string) error
}
