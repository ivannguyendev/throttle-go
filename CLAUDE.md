# CLAUDE.md

This file guides Claude Code (claude.ai/code) in this repository.

## Project

`github.com/ivannguyendev/throttle-go` is the Go port of the npm package `@ivannh/throttle-window`. It is a keyed leading + trailing throttle with a pluggable window store: the in-memory `MemoryEngine` by default, and the separate `redisengine` module (go-redis v9) for clusters.

- User documentation is in `README.md`.
- The decision log is `docs/design-decisions.md` (G1–G19, mapped to TS D-numbers). Read it before you change behaviour.

## Commands

You need Go 1.26 or newer. There are two modules: `.` (core, stdlib only) and `redisengine/`. The root holds only repository configuration; the core package lives in `throttle/` (import `github.com/ivannguyendev/throttle-go/throttle`).

| Task | Command |
| --- | --- |
| Test | `make test` (`go test -race -shuffle=on -count=1 ./...` in both modules) |
| One test | `go test -race -run TestName ./throttle` |
| Allocation budgets | `make alloc` (files tagged `!race`) |
| Lint | `make lint` (go vet + golangci-lint v2.14.0, `.golangci.yaml`) |
| Vulnerabilities | `make vuln` |
| Benchmarks | `make bench` |
| Real Redis | `make integration` (Docker), or `REDIS_ADDR=host:port go test ./...` in `redisengine/` |

Before a PR, run `make lint test` and use Conventional Commits.

## Architecture

```text
throttle/ (flat package; paths below are relative to it)
Window[T]  window.go (New/Close/Shutdown/Stats), window_entry.go (Exec/Wait/enter)
  → scheduler_attempt.go  attempt → beginRun → run → endRun (leading + trailing)
  → scheduler_timer.go    onTimer (window end), armForRemaining (lost race), recoverState
  → scheduler_state.go    keyState, cell[T], arm, cleanup (all under the shard lock)
  → lease.go              self-rescheduling extend while fn runs
  → engine_guard.go       breaker + fail-open + engine panic containment
  → Engine                MemoryEngine | redisengine.Engine | custom
```

## Invariants to preserve

- **Never hold a shard lock across an engine call or `fn`.** Log after you unlock.
- **Use the two critical sections (G4).** `enter` and `beginRun` must each stay a single lock section.
- **Check liveness after every blocking call.** Re-lock and check `w.alive(s)` before touching state (D13).
- **`armed` and `busy` are never both true (G9).** Only the flow that holds `busy` calls `arm`.
- **Add to `w.inflight` only under the shard lock**, while the state is alive (G13).
- **Engine calls use `w.engineCtx`.** A cancelled context never counts as an engine failure (G5).
- **The core imports only the standard library.** Test-only dependencies are fine.
- **Coalesced paths stay at 0 allocations.** `throttle/window_alloc_test.go` pins the budgets. Raising a budget needs a reason.

## Conventions

- **Files and size.** Go source goes in `throttle/`, `internal/` or `redisengine/`, never the repository root. Files are snake_case and named for what they do; keep each under ~200 lines.
- **Doc comments.** Every export has one, stating concurrency safety and ownership. Comments sit above code, never trailing.
- **Errors.** Use the sentinels `ErrClosed`/`ErrDropped`/`ErrPanicked`/`ErrInvalidArgument`, wrapped in `*KeyError` for runtime outcomes. Argument errors wrap `ErrInvalidArgument`.
- **Logging.** Use `slog`, always through `LogAttrs` behind an `Enabled` guard.
- **Tests.** Black-box tests (`package throttle_test`) run inside `synctest.Test` bubbles. Fake engines that hang block on `ctx.Done()` (`testutil.Hang`). Call `synctest.Wait()` before you assert on logs written after a waiter is released. Internal tests (`package throttle`) are for the codec, allocation budgets and the owned engine.
- **Decision log.** It is append-only, and README and `docs/design-decisions.md` must stay in sync with behaviour.
