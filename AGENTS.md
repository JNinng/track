# AGENTS.md — track

`track` is a **deterministic, journal-driven workflow engine** for Go. Every
workflow action (execute / sleep / await) appends an immutable log entry; after
a crash the engine replays the journal and reproduces the exact same execution.
**Determinism is the central invariant of this codebase — most edits must not
break replay.**

- Module: `github.com/jninng/track` (Go 1.26.2)
- Design contract: **`docs/track.md` is the source of truth.** Read it before
  changing anything in `engine/`, `model/`, `store/`, `clock/`, or `policy/`.
  Sections are referenced from code comments ("设计文档第 N 节").

## Commands

```bash
go test ./... -race     # all tests, race detector ON (always use -race)
go test ./engine -run TestName -race   # focused test
go vet ./...            # static checks
go run ./examples       # run the hello workflow end-to-end
```

One dependency: `github.com/jninng/observ` (zero-dep Logger/Meter contract module,
used by `engine` for logs & metrics). No other external deps / no Makefile / no
codegen. Single import root — the prometheus dep tree stays isolated in the
separate `examples/observability` module.

## Directory map

```
engine/    core engine: API (engine.go), primitives (context.go),
           run loop (runner.go), worker pool (scheduler.go),
           sentinel errors (errors.go), options (options.go), test hooks (testing.go)
model/     domain types: EntryKind, RunID, Signal, LogEntry, RunMeta/RunStatus
store/     storage INTERFACES only: Reader/Writer/Mailbox/Locker/Meta, aggregate Interface
infra/memory/   in-memory implementation (tests); New()/NewLocker()/NewMailbox()
clock/     Clock interface + RealClock / FakeClock
policy/    RetryPolicy interface + NoRetry/FixedDelay/ExponentialBackoff
examples/  hello_workflow.go end-to-end demo;
           examples/replay determinism replay demo;
           examples/observability SEPARATE module (own go.mod, replace to root)
           wiring observ.Logger(slog) + observ.Meter(prom adapter) — keeps the
           prometheus dep tree out of the root module
docs/track.md   the contract
```

## Architecture boundaries (layer rules)

- `engine` depends **only on the `store` interfaces** (esp. `store.Interface`),
  never on `infra/*`. New backends (mysql/redis) live under `infra/` and satisfy
  `store.Interface`.
- `model` is a leaf — no deps on engine/store/infra.
- Business workflow code talks to the engine **exclusively** through the injected
  `*engine.WorkflowContext` accessors and primitives. It must not touch internal
  fields, storage, or clocks directly.
- Dependency injection is explicit (WorkflowContext param, option funcs,
  `WithClock`/`WithWorkers`/`WithQueueSize`/`WithRecoverInterval`/
  `WithLogger`/`WithMeter`). No `context.Value`.

## Determinism / replay invariants — DO NOT BREAK

These are easy to break silently. Each is a hard contract in `docs/track.md`:

- **Positional journal.** Identity of an entry is *position + EntryKind*. Replay
  consumes the next entry by position, matching on Kind. **Never** look up
  journal entries by string key. Loops work because each iteration consumes one
  entry — no counters/suffixes.
- **Labels are cosmetic.** `Label`/`ExecutionConfig.Label` is metadata for humans
  and **never** participates in replay matching. Changing a label must not break
  replay of existing journals.
- **Execute records success only.** A failed step writes **no** log; on recovery
  it re-executes. `LogEntry.Err` in a KindExec record is always empty.
- **Return records its terminal result.** `Return` appends a `KindReturn` entry
  (payload = marshaled result); on replay the recorded payload is authoritative
  and the freshly-computed argument is **ignored**. This closes the determinism
  hole where a non-deterministic `Return` argument (e.g. `time.Now()`, a counter)
  would diverge across crash-recovery replay. A plain `return nil` completion
  writes **no** entry — only an explicit `Return` does. Adding/removing a `Return`
  is a journal-shape change; bump `WithVersion` when it must fail loud on old journals.
- **commit-before-Ack.** `Await` must persist the `KindAwait` decision *before*
  calling `Mailbox.Ack`. An `Ack` failure is non-fatal (orphan signal, leak not
  determinism bug) — never fail the workflow on it.
- **Wakeup deadline is selected by sentinel.** `ErrSleeping`→`sleepDeadline`,
  `ErrAwaiting`→`awaitDeadline`. Do not pick "any nonzero" deadline, or a
  finished Sleep's stale value wakes an Await immediately (busy-loop).
- **Register wakeup timers in the synchronous scheduling phase**, not in a
  background goroutine, so the deadline is anchored deterministically.
- **Divergence guards.** `consume` Kind-mismatch → `ErrJournalMismatch`. Also a
  terminal return (`nil`/`ErrReturn`) with `cursor != len(journal)` →
  `ErrJournalMismatch`. Suspended paths (ErrSleeping/ErrAwaiting) skip guard (b).
- **Breaking code change → bump version** via `Register(..., WithVersion(v))` so
  replay fails loudly with `ErrVersionMismatch` instead of corrupting silently.

## Sentinel errors

Defined in `engine/errors.go`. Business code never constructs them; it uses the
`IsXxx(err)` helpers (all `errors.Is`-based, wrap-safe): `IsSleeping`,
`IsAwaiting`, `IsAwaitTimeout`, `IsReturn`, `IsVersionMismatch`,
`IsJournalMismatch`. Note: terminal errors persisted to `RunMeta.Err` are stored
as **strings** — `errors.Is` can't recover the chain, so match on message
content (tests use `strings.Contains`).

## Testing conventions (mandatory)

- **Always inject `clock.FakeClock`** in tests. Never use real `time.Sleep` to
  verify Sleep/timeout logic — use `clk.Advance(d)`.
- **Record & Replay** is required for workflow logic, not just happy-path:
  assert that a replayed run makes **0** side-effect calls and that logs match
  the golden run via `logsEqual` (Kind/Label/Payload/Err, in order).
- Test hooks live in `engine/testing.go`: `SetTestOptions(TestOptions{RunSync:
  true, NoAutoRecover: true})` and `RunOnce(ctx, runID)` (simulate
  crash-restart replay). Production uses the async worker pool.
- Concurrency tests: verify `Locker` mutual exclusion + lease expiry/takeover
  (`memory.Locker` exposes `IsLocked`/`Expire`/`SetNow`). Run everything with
  `-race`.

## Coding conventions

- **Comments and doc comments are written in Chinese** (Simplified). Match this
  when editing — keep the `// 设计文档第 N 节` cross-references intact.
- Option-function pattern everywhere (`EngineOption`, `Option`, `MetaOption`,
  `RegisterOption`). Prefer it over config structs for optional params.
- Primitives use type parameters (`Input[T]`, `Execute[R]`). `WorkflowContext`
  methods (`Sleep`, `Await`, `Return`) use plain signatures.
- JSON (`encoding/json`) is the only serialization for LogEntry payloads and
  inputs/outputs. Keep payloads stable across versions for replay fidelity.

## Out of scope (not in this contract)

`infra/redis`, `infra/mysql`, and the `Cancel` primitive / `StatusCancelled`
trigger API are **not** implemented here. Anything under `infra/` other than
`memory` must satisfy the §4 store interface contract before wiring into the engine.
