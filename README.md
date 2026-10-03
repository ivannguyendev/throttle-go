# throttle-go

[![Go Reference](https://pkg.go.dev/badge/github.com/ivannguyendev/throttle-go/throttle.svg)](https://pkg.go.dev/github.com/ivannguyendev/throttle-go/throttle)
[![CI](https://github.com/ivannguyendev/throttle-go/actions/workflows/ci.yml/badge.svg)](https://github.com/ivannguyendev/throttle-go/actions/workflows/ci.yml)
[![license](https://img.shields.io/github/license/ivannguyendev/throttle-go.svg)](LICENSE)

`throttle-go` is a keyed leading + trailing throttle for Go. The window store is pluggable: use Redis when several instances must share a throttle, or keep it in memory for a single process. The first call for a key runs immediately, and all the calls made during the window collapse into exactly one trailing run at the end of the window.

It is the Go port of the npm package [`@ivannh/throttle-window`](https://github.com/ivannguyendev/throttle). The behaviour is the same, but the internals are redesigned for Go:
- per-key state is split across lock shards;
- the coalesced path makes no allocations;
- every goroutine has an owner;
- time is tested with `testing/synctest`.

## Contents

- [Features](#features)
- [Install](#install)
- [Quick start](#quick-start)
- [Redis and clusters](#redis-and-clusters)
- [How it works](#how-it-works)
- [API](#api)
- [Engines](#engines)
- [Failure handling](#failure-handling)
- [Errors](#errors)
- [Performance](#performance)
- [Caveats](#caveats)
- [Architecture](#architecture)
- [Development](#development)
- [CI](#ci)
- [Release](#release)
- [License](#license)

## Features

- **Leading + trailing, per key.** The first call runs now. A burst of calls collapses into exactly one trailing run at the end of the window.
- **Runs of a key never overlap.** Each window starts when a run *finishes*. Long runs hold a lease, so a key never runs twice at once, even across instances.
- **Cluster or single process.** `redisengine` (go-redis v9) deduplicates runs across instances. The default `MemoryEngine` needs nothing.
- **Fail-open.** A circuit breaker skips a failing engine. If Redis is down, jobs still run, and each instance keeps throttling on its own.
- **Wait for the result.** `Wait` returns the typed value of the run that covers your call. Bound the wait with a context deadline.
- **Fast.** A coalesced call takes about 26 ns and makes 0 allocations. Locks are sharded across keys, `Stats()` is O(1), and logging costs nothing when it is disabled.
- **Small API.** The core package depends only on the standard library. A custom engine needs three Redis-style methods.

## Install

```bash
go get github.com/ivannguyendev/throttle-go

# Only if you use Redis (a separate module, so the core stays dependency-free)
go get github.com/ivannguyendev/throttle-go/redisengine
```

Import the package from `github.com/ivannguyendev/throttle-go/throttle`. The `throttle/` directory keeps the Go source apart from the repository's configuration files.

Requires Go 1.26 or newer.

## Quick start

```go
package rooms

import (
	"context"
	"time"

	"github.com/ivannguyendev/throttle-go/throttle"
)

// The default engine is an in-memory MemoryEngine (single process).
var roomSync, _ = throttle.New[Room]("sync:room:", throttle.Config{Window: time.Second})

// Called on every change event, possibly many times per second per room.
func OnRoomChanged(ctx context.Context, roomID string) error {
	ran, err := roomSync.Exec(ctx, roomID, syncRoom)
	// ran == true:  this call ran syncRoom now (leading) and it has finished.
	// ran == false: coalesced; the trailing run at the end of the window covers it.
	_ = ran
	return err
}

// Need the result? Wait for the run that covers this call.
func SyncedRoom(ctx context.Context, roomID string) (Room, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return roomSync.Wait(ctx, roomID, syncRoom)
}

func syncRoom(ctx context.Context, phase throttle.Phase) (Room, error) {
	// phase is throttle.Leading or throttle.Trailing.
	return loadAndPublishRoom(ctx)
}

// On shutdown: stop the timers and wait for the runs in flight.
func Stop(ctx context.Context) error { return roomSync.Shutdown(ctx) }
```

## Redis and clusters

```go
import (
	"github.com/redis/go-redis/v9"

	"github.com/ivannguyendev/throttle-go/throttle"
	"github.com/ivannguyendev/throttle-go/redisengine"
)

client := redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	// The throttle has no engine timeout: let the client fail fast instead.
	ReadTimeout:  500 * time.Millisecond,
	WriteTimeout: 500 * time.Millisecond,
})

// One engine can back many Windows. The key prefix keeps their keys apart.
engine, err := redisengine.New(client) // *redis.Client, *redis.ClusterClient, *redis.Ring, UniversalClient

roomSync, err := throttle.New[Room]("sync:room:", throttle.Config{Engine: engine})
userSync, err := throttle.New[User]("sync:user:", throttle.Config{Engine: engine, Window: 5 * time.Second})

_, err = roomSync.Exec(ctx, roomID, syncRoom)

// Shutdown: shut down the Windows, then close the client you own.
_ = roomSync.Shutdown(ctx)
_ = userSync.Shutdown(ctx)
_ = client.Close()
```

- **Key and value.** The Redis key for a job is `keyPrefix + key`, for example `sync:room:42`. It is a plain string with a `PX` expiry, so there is nothing to clean up. Its value is the time the window ends, in epoch milliseconds.
- **Shared settings.** Every instance that should share windows must use the same key prefix and the same `Window`.
- **One run per window across the cluster.** Pending calls live in each instance's memory. An instance that loses the window race keeps its pending calls, reads when the window ends with `GET`, and retries at that time. After 10 losses in a row, those calls fail with `ErrDropped`.

## How it works

```text
Window = 1s, fn takes 300 ms

t=0.0  call #1 -> runs now (leading)
t=0.3          -> run done, window [0.3 -> 1.3)
t=0.5  call #2 -> coalesced
t=0.9  call #3 -> coalesced
t=1.3          -> one trailing run, covers #2 and #3
t=1.6          -> run done, window [1.6 -> 2.6)
t=2.6          -> window passed with no calls: closed, key forgotten
t=3.0  call #4 -> runs now (leading)
```

- **Leading.** The first call for an idle key runs immediately (`throttle.Leading`).
- **Trailing.** Calls made during an open window are coalesced into exactly one run at the end of the window (`throttle.Trailing`).
- **No overlap.** Every run opens a new window that starts when the run **finishes**, so runs of one key never overlap.
- **Closing.** A window that passes with no calls closes. The next call leads again.
- **Latest fn wins.** The trailing run executes the latest `fn` passed for that key.
- **Covered calls.** A run covers a call if the call arrived **before the run started executing `fn`**. A call that arrives while `fn` is executing (t=0.1 or t=1.4 above) is covered by the next trailing run.

What each caller in the timeline gets:

| Call   | `Exec`                 | `Wait`                                 |
| ------ | ---------------------- | -------------------------------------- |
| #1     | `true` at t=0.3        | value of the leading run, at t=0.3     |
| #2, #3 | `false` immediately    | value of the trailing run, at t=1.6    |
| #4     | `true` at t=3.3        | value of its own leading run, at t=3.3 |

## API

Full reference: [pkg.go.dev](https://pkg.go.dev/github.com/ivannguyendev/throttle-go/throttle).

### `throttle.New[T](keyPrefix string, cfg Config) (*Window[T], error)`

`keyPrefix` is required and namespaces the throttle: engine keys are `keyPrefix + key`. `T` is the result type of the throttled function; use `struct{}` if you only call `Exec`. `New` starts no goroutine.

| `Config` field | Type | Default | Description |
| --- | --- | --- | --- |
| `Window` | `time.Duration` | `1s` | Window length, measured from the end of each run. Minimum `1ms`. |
| `Engine` | `throttle.Engine` | owned `MemoryEngine` | Window store. The default engine belongs to the Window and is cleared on `Close`. An engine you pass in is never cleared. |
| `Name` | `string` | `keyPrefix` without a trailing `:` | Value of the `throttle` attribute on log records. |
| `Logger` | `*slog.Logger` | discard | `nil` logs nothing, not even errors. |

What gets logged:
- at `Info`: every run, with its phase;
- at `Error`: failed trailing runs and runs that panicked;
- at `Debug`: the breaker opening, the rearm limit, and an unusually large key map (more than 5,000 keys, at most once a minute).

Invalid arguments return an error that wraps `ErrInvalidArgument`.

### `(*Window[T]).Exec(ctx, key, fn) (bool, error)`

`fn` has the type `throttle.Func[T]`, that is, `func(ctx context.Context, phase throttle.Phase) (T, error)`.

| Case | Result |
| --- | --- |
| **Leading**: the key is idle | Runs `fn(ctx, Leading)` on the calling goroutine, waits for it, and returns `true` together with `fn`'s error (backpressure). A panic in `fn` is re-raised here, after the key's state has been restored. |
| **Coalesced**: the key is running or inside a window, on any instance | Returns `false` immediately, with no allocation. The trailing run covers this call. |

A coalesced `Exec` never sees the outcome of the trailing run. A failed trailing run rejects the `Wait` calls it covers and is logged at `Error`.

### `(*Window[T]).Wait(ctx, key, fn) (T, error)`

`Wait` returns the value of the run that covers this call, either the leading or the trailing run, or that run's error. The trailing run uses the latest `fn` for the key, so the value may come from a newer closure than the one you passed. That is safe as long as handlers recompute from source.

When `ctx` ends first, `Wait` returns a `*KeyError` that wraps `ctx.Err()`. The run is **not** cancelled: it still runs and still covers the call. A leading run started by `Wait` gets the caller's context values but not its cancellation, because other waiters share that run.

### `Stats()`, `Close()`, `Shutdown(ctx)`

| Method | Description |
| --- | --- |
| `Stats()` | `Stats{Active, Pending, Keys, Timers}`. O(1), lock-free counters. `Active` includes keys still waiting on the engine. |
| `Close()` | Stops every timer and makes calls that have not run yet fail with `ErrClosed`. Clears the default `MemoryEngine`, never an engine you passed in. Does not cancel or wait for runs already executing `fn`. Idempotent. |
| `Shutdown(ctx)` | Calls `Close`, then waits until every run the Window started itself (trailing runs and the leading runs of `Wait`) has finished, or until `ctx` ends. The leading runs of `Exec` run on their caller's goroutine. |

`Exec` and `Wait` after `Close` return `ErrClosed`. Every method is safe for concurrent use.

## Engines

### `MemoryEngine` (default)

`MemoryEngine` is for a single process:
- **Clock.** Expiry uses Go's monotonic clock, so wall-clock jumps don't affect it.
- **Sharding.** Keys are spread over 32 lock shards.
- **Sweeper.** A lazy sweeper (one `time.AfterFunc`, no goroutine) purges expired keys, and runs only while keys exist. Tune it with `NewMemoryEngine(MemoryEngineConfig{SweepInterval: ...})`; the default is `1m`.
- **Clear.** `Clear()` empties the engine, and it keeps working afterwards.

To share one in-process store between several Windows, create one `MemoryEngine` and pass it to each of them.

### `redisengine`

`redisengine.New(client)` accepts any go-redis v9 client that implements `Get`, `SetArgs` and `Del`. It stores one auto-expiring key per throttle key and uses only single-key commands, so it works with Cluster, Ring and Sentinel.

| Method | Redis command |
| --- | --- |
| `Get(key)` | `GET key` (`redis.Nil` → not found, not an error) |
| `Set(key, value, {TTL, NX: true})` | `SET key value PX ms NX` (a lost race → `false, nil`) |
| `Set(key, value, {TTL})` | `SET key value PX ms` |
| `Del(key)` | `DEL key` |

TTLs are rounded up to whole milliseconds and are never 0, because go-redis treats a TTL of 0 as "no expiry".

### Custom engines

Implement `throttle.Engine`. Keys arrive already prefixed.

```go
type SetOptions struct {
	TTL time.Duration // expiry
	NX  bool          // only set if the key is missing or expired
}

type Engine interface {
	Get(ctx context.Context, key string) (value string, found bool, err error)
	Set(ctx context.Context, key, value string, opt SetOptions) (bool, error)
	Del(ctx context.Context, key string) error
}
```

| Method | Contract |
| --- | --- |
| `Get` | The stored value, or `found == false` when the key is missing or expired. |
| `Set` | Store `value` with a `TTL` expiry. With `NX`, write **atomically and only if the key is missing or expired**, and return `true` only if this call wrote it. Without `NX`, always overwrite, **even an expired key**, and return `true`. |
| `Del` | Remove the key. The Window never calls it; it exists for your own use. |

The Window stores the window's end time (epoch ms) as the value, and reads it back with `Get` to know when to retry after losing a window race. The atomic `NX` is what stops two instances from running the same key.

Return an error on failure, and don't retry inside the engine: the Window handles circuit breaking and fail-open, and it also turns a panic in the engine into a failure. Implementations must be safe for concurrent use. There is no timeout around engine calls, so give your client its own. The context passed to the engine is cancelled by `Close`.

## Failure handling

| Mechanism | Behaviour |
| --- | --- |
| No engine timeout | The Window waits as long as each engine call takes, so a slow engine slows the throttle. Set `ReadTimeout`/`WriteTimeout` on go-redis: a call that times out counts as an engine error and the job fails open. `Close` cancels engine calls still in flight. |
| Circuit breaker | 5 consecutive engine failures open the breaker for 10 s, and the engine is skipped. Then a single probe call decides: success closes the breaker, failure opens it for another 10 s. A call cut short by `Close` is not counted. |
| Fail-open | On an engine error, an engine panic, or an open breaker, the job **still runs**. In-memory state keeps throttling per instance, so during a Redis outage each instance runs a key at most once per window. |
| Lease | While `fn` runs, the window is extended every `max(Window/2, 100ms)`. At most one extend is ever in flight, and the last one finishes before the window is reopened, so another instance can't overlap a run that lasts longer than `Window`. A Window with its own private `MemoryEngine` skips the lease, because nobody else can see that engine. |
| Lost window race | The losing instance reads the window's end time with `Get` and retries then. The wait is capped at the lease TTL, and an unreadable value falls back to `Window`. |
| Run errors | A failed leading `Exec` returns `fn`'s error. A failed trailing run fails the `Wait` calls it covered and is logged at `Error`. |
| Panics in `fn` | Leading `Exec`: re-raised on the caller's goroutine. Runs on the Window's own goroutines (trailing runs, and leading runs started by `Wait`): converted to `ErrPanicked`, delivered to the covered waiters, and logged. The process never crashes. |

## Errors

| Error | When |
| --- | --- |
| `ErrClosed` | `Close` was called before the call could run, or `Exec`/`Wait` was called after `Close`. |
| `ErrDropped` | A pending call gave up after losing the window race to other instances more than 10 times in a row. |
| `ErrPanicked` | The run covering a `Wait` panicked on a goroutine owned by the Window. |
| `ErrInvalidArgument` | Invalid key prefix, `Config`, key or `fn`. |
| `*KeyError{Key, Err}` | Wraps the four errors above, and the context error of `Wait`, with the key. |

```go
room, err := roomSync.Wait(ctx, roomID, syncRoom)
switch {
case errors.Is(err, context.DeadlineExceeded):
	// Still running; the result arrives later, so the caller moves on.
case errors.Is(err, throttle.ErrDropped):
	// Another instance kept winning the window.
case err != nil:
	if ke, ok := errors.AsType[*throttle.KeyError](err); ok {
		log.Printf("throttle key %s: %v", ke.Key, ke.Err)
	}
}
```

## Performance

These numbers come from `make bench` on a 12-core laptop with Go 1.27. Run it on your own hardware to compare.

| Benchmark | ns/op | allocs/op |
| --- | --- | --- |
| `Exec`, coalesced, one hot key, parallel | ~112 | 0 |
| `Exec`, coalesced, 1,024 keys, parallel | ~26 | 0 |
| `Exec`, leading, new key | ~1,900 | 6 |
| `MemoryEngine.Set` NX, parallel | ~124 | 0 |

How it gets there:
- **Lock sharding.** Per-key state lives in 32 lock shards (`hash/maphash`), each padded to its own cache line, so calls on different keys don't contend.
- **Shared result cell.** Every waiter of a run shares one result cell, whose channel is closed to broadcast the outcome. A waiter costs no allocation, and a waiter whose context ends leaves nothing behind.
- **No goroutine per key.** There is one reusable timer per key, and leases use self-rescheduling `time.AfterFunc`.
- **Cheap stats and logs.** Stats are atomic counters. Logging uses `slog.LogAttrs`, guarded by `Enabled`.
- **Pinned budgets.** Allocation budgets are pinned by tests (`throttle/window_alloc_test.go`), so a regression fails CI.

A closure passed as `fn` that captures variables is allocated by the caller, as with any Go closure. Use a method value or a function you create once to keep the coalesced path at zero allocations.

## Caveats

- **Pending trailing runs are lost on exit.** `Close`/`Shutdown` reject calls that have not run yet. Keep your own safety net, such as a periodic full sync.
- **Pending state lives in RAM only.** Redis stores only the window key. Pending calls don't survive a crash or restart.
- **No re-entrant waits.** Inside a key's `fn`, don't `Wait` for the **same key**. The covering run can't start until the current one finishes, so the wait deadlocks until its context ends, or forever if the context never ends.
- **A hung engine stalls its key.** There is no engine timeout. If an engine call never returns, the key stays busy: a leading `Exec` waits with it, and later calls coalesce into a run that never comes. Configure client timeouts, and give `Wait` a deadline where a caller must not hang.
- **Silent by default.** Without a `Logger`, nothing is logged, not even failed trailing runs, and a caller that only uses `Exec` never sees them. Pass a logger in production.
- **Trailing runs are not capped across keys.** Many keys whose windows end together run their trailing `fn` at the same time, each on its own timer goroutine.
- **Retry timing reads wall clocks.** The window's end time is stored in epoch ms, so clock skew between hosts shifts when a losing instance retries (the shift is capped at the lease TTL). It never lets two instances run the same key, because `NX` decides that.
- **Latest closure wins.** A trailing run, and every `Wait` it covers, uses the most recent `fn` for the key. Write handlers that recompute from source.
- **Leading runs apply backpressure.** `Exec` waits for a leading run and returns its error.

## Architecture

```text
Window (throttle/window.go, window_entry.go)  public API: New, Exec, Wait, Stats, Close, Shutdown
  ├─ shards [32]{mutex, map[key]*keyState}     per-key state (scheduler_state.go)
  ├─ attempt / run / endRun                    leading + trailing state machine (scheduler_attempt.go)
  ├─ onTimer / armForRemaining                 window end, lost-race retry (scheduler_timer.go)
  ├─ lease                                     extends the window while fn runs (lease.go)
  └─ engineGuard                               breaker + fail-open + panic containment (engine_guard.go)
       └─ Engine: MemoryEngine (memory_engine.go) | redisengine | custom
```

```text
├── go.mod, Makefile, .golangci.yaml, README.md, CLAUDE.md, LICENSE   repository configuration only
├── throttle/               core package `throttle` (stdlib only), flat: window_*, scheduler_*, engine_*
├── internal/breaker/       consecutive-failure circuit breaker with one probe
├── internal/shard/         maphash shard index shared by Window and MemoryEngine
├── internal/testutil/      StubEngine (scripted, hangable) and a recording slog logger
├── redisengine/            separate module: go-redis v9 adapter (+ miniredis and real-Redis tests)
├── docs/design-decisions.md   decision log, mapped to the TypeScript original
└── .github/workflows/      ci.yml, release.yml
```

The rules that keep the state machine correct are listed in [`docs/design-decisions.md`](docs/design-decisions.md):
- two critical sections;
- a liveness check after every blocking call;
- no lock ever held across an engine call or `fn`.

## Development

Requirements:
- Go 1.26 or newer;
- Docker, optional, for the real-Redis integration test.

```bash
git clone https://github.com/ivannguyendev/throttle-go.git
cd throttle-go
make test
```

| Command | Description |
| --- | --- |
| `make test` | `go test -race -shuffle=on` for both modules |
| `make alloc` | Allocation budgets (excluded from race builds) |
| `make bench` | Benchmarks with allocation counts |
| `make cover` | Coverage of the core module |
| `make fuzz` | Fuzz the engine value parser for 30 s |
| `make lint` | `go vet` + golangci-lint v2 (`.golangci.yaml`) for both modules |
| `make vuln` | `govulncheck` for both modules |
| `make tidy` | `go mod tidy` for both modules |
| `make integration` | Start `redis:7-alpine` in Docker and run `redisengine` against it |

- **Test style.** Timing tests run inside `testing/synctest` bubbles: the clock is fake, so they are instant and deterministic. Every package checks for leaked goroutines with `goleak`.
- **Module layout.** `redisengine/go.mod` keeps `replace github.com/ivannguyendev/throttle-go => ../`, so it always builds against the core in this checkout. Go ignores `replace` in dependencies, so users get the version in its `require` line.
- **Before a PR.** Run `make lint test` and use [Conventional Commits](https://www.conventionalcommits.org/).

## CI

`.github/workflows/ci.yml` runs on every push to `main` and every pull request.

| Job | What it runs |
| --- | --- |
| Lint (per module) | `go mod tidy -diff`, `go vet`, golangci-lint, govulncheck |
| Test (Go 1.26 and 1.27 × module) | race + shuffle tests, allocation budgets, a single benchmark pass, a coverage summary |
| Fuzz | `FuzzParseWindowEnd`, 30 s |
| Integration | `redisengine` against a `redis:7-alpine` service container |

## Release

Go modules are published through git tags: `proxy.golang.org` and `pkg.go.dev` pick up a tag once something requests it. A published version is permanent, so `.github/workflows/release.yml` creates the tag only after the full CI pipeline has passed on that commit.

Run it from the **Actions** tab, using **Release** → **Run workflow** on `main`:

| Input | Values |
| --- | --- |
| `module` | `core` (tag `vX.Y.Z`) or `redisengine` (tag `redisengine/vX.Y.Z`) |
| `version` | `v0.1.0`, `v0.2.0-rc.1`, … |

The workflow then:
1. runs CI;
2. validates the version and checks that the tag does not exist yet;
3. creates an annotated tag;
4. creates a GitHub release with generated notes (marked as a prerelease if the version contains `-`);
5. requests the version through `proxy.golang.org`, so it shows up on pkg.go.dev right away.

No secrets are needed.

### Releasing both modules

`redisengine` depends on a published core, so always release the core first:

1. Release `core` `v0.1.0`.
2. Bump the requirement and merge it to `main`:

   ```bash
   cd redisengine
   go mod edit -require=github.com/ivannguyendev/throttle-go@v0.1.0
   go mod tidy
   git commit -am "chore(redisengine): require throttle-go v0.1.0"
   ```

3. Release `redisengine` `v0.1.0`. The workflow refuses to run if the core version that `redisengine` requires is not tagged, and it builds `redisengine` without the `replace` to prove that the version resolves.

### Retracting a bad version

Tags can't be unpublished. Release a new version whose `go.mod` contains a `retract`:

```go
retract v0.1.1 // Close could leave a timer running; use v0.1.2.
```

## License

[MIT](LICENSE)
