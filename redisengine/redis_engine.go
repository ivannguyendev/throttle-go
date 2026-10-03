// Package redisengine is a throttle.Engine backed by Redis through go-redis
// v9, so every instance that shares the Redis shares the throttle windows.
//
// It stores one auto-expiring string per throttle key and uses only
// single-key commands, so it works with a standalone server, Sentinel, a
// Cluster or a Ring:
//
//	Get                → GET key
//	Set (NX)           → SET key value PX ms NX
//	Set                → SET key value PX ms
//	Del                → DEL key
//
// The throttle has no timeout around engine calls: configure ReadTimeout and
// WriteTimeout on the client so a slow Redis fails fast and the throttle
// fails open.
package redisengine

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ivannguyendev/throttle-go/throttle"
)

// Client is the part of a go-redis client the engine uses. *redis.Client,
// *redis.ClusterClient, *redis.Ring and redis.UniversalClient all satisfy it.
type Client interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	SetArgs(ctx context.Context, key string, value any, a redis.SetArgs) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

// Engine implements throttle.Engine on Redis. It is safe for concurrent use
// and can back many Windows; give each Window its own key prefix. It never
// closes the client: the client belongs to the caller.
type Engine struct {
	client Client
}

var _ throttle.Engine = (*Engine)(nil)

// New wraps client.
func New(client Client) (*Engine, error) {
	if client == nil {
		return nil, errors.New("redisengine: client is required")
	}
	return &Engine{client: client}, nil
}

// Get implements throttle.Engine. A missing key is not an error.
func (e *Engine) Get(ctx context.Context, key string) (string, bool, error) {
	value, err := e.client.Get(ctx, key).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return "", false, nil
	case err != nil:
		return "", false, err
	default:
		return value, true, nil
	}
}

// Set implements throttle.Engine. A lost NX race is (false, nil), never an
// error, so normal contention between instances never trips the breaker.
func (e *Engine) Set(ctx context.Context, key, value string, opt throttle.SetOptions) (bool, error) {
	args := redis.SetArgs{TTL: ttl(opt.TTL)}
	if opt.NX {
		args.Mode = "NX"
	}
	err := e.client.SetArgs(ctx, key, value, args).Err()
	switch {
	case errors.Is(err, redis.Nil):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

// Del implements throttle.Engine.
func (e *Engine) Del(ctx context.Context, key string) error {
	return e.client.Del(ctx, key).Err()
}

// ttl rounds up to whole milliseconds and never returns 0: go-redis treats a
// zero TTL as "no expiry", which would lock a key across the whole cluster.
func ttl(d time.Duration) time.Duration {
	ms := (d + time.Millisecond - 1) / time.Millisecond
	return max(ms, 1) * time.Millisecond
}
