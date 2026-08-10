# cdcfresh

## What this is

cdcfresh is an embeddable Go library that keeps derived tables (rollups,
lookups, read models) fresh from change-data-capture events. It owns *when*
and *for which scope* a rebuild runs; you own every line of SQL. CDC events
are a doorbell, never data — a rebuild always recomputes its scope from
source tables, so duplicate events are harmless and drift is impossible by
construction.

## Build and test

```
make test              # every tier — unit + integration
make test-unit         # unit tier — no Docker required
make test-race         # unit tier with the race detector
make test-integration  # integration tier — needs Docker
make lint               # gofmt check + go vet
```

`go test ./...` (no build tag) is the unit tier: integration tests live
behind the `integration` build tag and are skipped by an untagged build.

## Hard constraints

Things that will break the project if violated:

- Root package imports **stdlib only**. A transport adapter (e.g. `pulsar/`)
  keeps its client dependency in its own subdirectory — never add a
  `require` to the root module for an adapter's sake.
- `Rebuild` and `Scope` are user callbacks, **never invoked while holding
  the Refresher's mutex** — all dirty-set access happens under that mutex.
- Events are doorbells: **no decoded event value may reach user SQL.**
- Conventional commits, each ending with:
  ```
  Generated with Claude Code
  Co-Authored-By: Claude <noreply@anthropic.com>
  ```
  Stage specific files — never `git add -A`.

## Layout

```
cdcfresh/            root package — import "github.com/bis-code/cdcfresh"
├── event.go         public contract: Key, RowEvent, Event, EventSource, ErrSkip
├── options.go       Option constructors + validation
├── refresher.go     Refresher, New, Run
├── loops.go         receive / schedule / worker / reconcile goroutines
├── coalesce.go      dirty-set state machine (pure, explicit clock)
├── backoff.go       retry delay
├── stats.go         atomic counters + Stats snapshot
├── internal/canaljson/ [planned] canal-json decoder — shared by all transports
├── pulsar/          [planned] Pulsar EventSource adapter
└── harness/         [planned] TiDB + TiCDC + Pulsar stack for the dev loop and integration tests
```

`docs/superpowers/` and `.superpowers/` are gitignored working artifacts —
they never get committed.

## Settled decisions — do not reopen

- API shape: functional options; `New` fails fast, reporting every missing
  or invalid option in one error.
- `Key` is an opaque string; the library never parses it.
- canal-json is decoded by a hand-written decoder in `internal/canaljson`,
  named for the wire format (not the transport) so a future Kafka adapter
  shares it with the Pulsar adapter.
- Coalescing: per-key debounce with a max-wait cap, FIFO-ready keys, a
  bounded worker pool, single-flight per key.
- Failures: per-key exponential backoff, then quarantine after
  `PoisonAfter` consecutive failures, healed only by reconcile.
- Acking: a source event is acked once its keys are enqueued, not after the
  rebuild that processes them completes.
- Reconcile: enumerates the live key universe through the normal pipeline,
  with one sweep at `Run` start and one per interval.
- `Stats()` is a zero-dependency snapshot struct; no metrics client in core.
- v1 supports a single active consumer (e.g. a Pulsar failover
  subscription); no cross-instance key routing.
- Adapters are subpackages of the root module until a second one exists,
  then become separate modules — the import path is unchanged either way.
- The integration tier is exactly one build tag, `integration` (not "e2e":
  a library has no end-to-end user-facing flow).

These are ratified. Reopening one needs an explicit decision, not a
drive-by change.

## Before pushing

Unit tier and lint must be green. CI runs the fast tier (gofmt, vet, unit
tests) on every push and pull request; the integration tier runs nightly and
on manual dispatch.
