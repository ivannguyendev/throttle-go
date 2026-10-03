# Design decisions

`throttle-go` ports [`@ivannh/throttle-window`](https://github.com/ivannguyendev/throttle) to Go. The public semantics are kept: leading/trailing runs, covered calls, fail-open, the breaker, the lease, and the rearm limit followed by a drop. The internals were redesigned for Go's multi-threaded runtime rather than translated from the single-threaded JavaScript event loop.

This log is append-only. To change a decision, add a new row that names the one it supersedes.

## Carried over from the TypeScript original

| TS | Decision | Go form |
| --- | --- | --- |
| D10 | The local claim comes before the remote acquire. | `busy = true` is set in `enter`, under the shard lock, before `guard.acquire`. |
| D12 | Waiters are registered synchronously, before any blocking call. | `enter` attaches the caller to `state.next` (the cell) in the same critical section as the claim. |
| D13 | A liveness check follows every await. | `w.alive(s)` (`!s.dead && !closed`) is checked after re-locking, after every engine call and every run. |
| D16 | All throttle logic lives in the scheduler; engines are thin Redis-style stores. | `Engine` has `Get`/`Set`/`Del`. `engineGuard` maps acquire → SET NX, extend → SET, remaining → GET. |
| D17 | The stored value is the window end in epoch ms. | `encodeWindowEnd` / `parseWindowEnd` (fuzzed). |
| D18 | No engine timeout; the job fails open. | The same, plus G5. |
| D22 | Only a leading run reports to its caller; a failed trailing run rejects its waiters and is logged at error. | The same; see G7 for panics. |
| D23 | `destroy()` clears only the default engine it created. | `Close` calls `Clear` on the owned `*MemoryEngine` only. `Clear` is not part of `Engine`. |

## Go-specific decisions

| # | Decision | Why |
| --- | --- | --- |
| G1 | The API is idiomatic Go: `New[T](prefix, Config)`, `Exec(ctx, key, fn) (bool, error)`, `Wait(ctx, key, fn) (T, error)`, `Stats()`, `Close()`, `Shutdown(ctx)`. A context deadline replaces `waitFinish().timeout()`. | The fluent builder duplicated what `context` already does. The user chose this API. |
| G2 | `Window[T]` is generic. | Results are stored typed, with no `any` boxing and no runtime type assertion. A non-generic Window would let functions of different types share one key and panic on the assertion. |
| G3 | Key state is split across 32 lock shards (`hash/maphash`), each padded to a cache line. No lock is ever held across an engine call or `fn`. | Calls on different keys don't contend, and a slow engine or a slow `fn` never blocks other keys. |
| G4 | There are two mandatory critical sections. Entry: closed check, ensure, pending, cell, claim. Run start: alive, rearms = 0, take the covered cell, pending = false, register the lease. | In a multi-threaded runtime, `Close` could otherwise slip between steps that the JavaScript event loop ran atomically. That would lose a waiter or leak a lease. |
| G5 | Engine calls use a Window-level context that `Close` cancels. A call cut short this way does not count against the breaker (`RecordIgnored` also releases a pending probe). | With a hung engine, goroutines are released on `Close`, and shutting down never trips the breaker. |
| G6 | Waiters share one `cell[T]` per run, whose `done` channel is closed to broadcast the outcome. | Per-waiter channels plus a waiter slice would allocate for every waiter, and a waiter that gave up on its context would stay in the slice. |
| G7 | Panics are handled as follows. A panic in a leading `Exec` is re-raised on the caller's goroutine after the state is restored. A panic in a run on a goroutine owned by the Window becomes `ErrPanicked` for the covered waiters and is logged. A `runtime.Goexit` in `fn` still settles waiters and reopens the window. | This follows the `singleflight` convention. A user function must never crash the process from a goroutine it does not own. |
| G8 | `engineGuard` turns an engine panic into an engine error, which counts towards the breaker and fails open. | Engine calls also run on timer and lease goroutines. A buggy client must not crash the service. |
| G9 | Each key has one reusable `time.AfterFunc` timer. No generation token is needed, because `armed` and `busy` are never both true: only the flow that holds `busy` arms the timer, right before it releases `busy`. | One allocation per key instead of one per window. A late callback after `Stop` is caught by `!armed` or `!alive`. |
| G10 | The lease is a self-rescheduling `AfterFunc`. It reschedules only after an extend returns. `stop` waits for an extend still in flight, outside every lock, before the closing extend. | At most one extend is ever in flight. With a pooled Redis client, a late lease extend could otherwise land after the closing extend and stretch the window. |
| G11 | A Window with its own private `MemoryEngine` skips the lease. | Nobody else can see that engine, and the local `busy` flag already prevents overlap. |
| G12 | `Stats` uses atomic counters updated at each transition. `Timers` counts window timers plus leases; deadlines are now contexts and are not counted. | O(1) and lock-free, where the original scanned every key. |
| G13 | `Shutdown(ctx)` calls `Close` and then waits for the runs the Window started itself (trailing runs and the leading runs of `Wait`), tracked by a WaitGroup whose `Add` happens under the shard lock. | The owner can wait for the goroutines it caused. The `Add` never races with `Wait`. |
| G14 | `MemoryEngine` is sharded, its sweeper is a lazy `AfterFunc` that runs only while keys exist, and its size counter changes under the shard lock. | No goroutine stays alive in an idle or shared engine. Concurrent `Set`/`Clear` cannot leave keys without a sweeper. |
| G15 | `redisengine` is a separate module that uses go-redis v9 through a 3-method consumer interface. `redis.Nil` means "not found" or "lost race". TTLs are rounded up to at least 1 ms. | The core keeps zero runtime dependencies. Normal contention must never trip the breaker, and a TTL of 0 means "no expiry" in go-redis. |
| G16 | `redisengine/go.mod` keeps `replace => ../`, and the release workflow checks that the core version it requires is tagged. | Development and CI always test against the core in the same checkout, while consumers ignore the `replace`. |
| G17 | Releases run through `workflow_dispatch`: CI, then the tag, then a GitHub Release, then a proxy warm-up. The first versions are `v0.1.0` for both modules. | A tag on the Go proxy is permanent, so CI must pass before the tag exists. Starting at v0 leaves room to adjust the API before v1. |
| G18 | Tests use `testing/synctest` for time, `goleak`, `-race -shuffle=on`, allocation budgets (`!race`), a fuzzed parser, and miniredis plus a real-Redis integration job. | The tests are deterministic and fast, and leaks, races and allocation regressions fail CI. |
| G19 | The core package lives in `throttle/` (import `github.com/ivannguyendev/throttle-go/throttle`). `go.mod` stays at the root, so the module path and the `vX.Y.Z` tags are unchanged. | The repository root holds only configuration (Makefile, lint, CI, docs). The package stays flat inside `throttle/`. |
