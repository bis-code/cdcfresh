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
make test-integration  # integration tier — needs Docker (starts its own containers)
make lint              # gofmt check + go vet
```

`go test ./...` (no build tag) is the unit tier: integration tests live
behind the `integration` build tag and are skipped by an untagged build.

## Hard constraints

Things that will break the project if violated:

- Root package imports **stdlib only** — the invariant is the *import graph*,
  not `go.mod`. CI enforces it with `go list -deps .`, which is the only check
  that stays honest: `go.mod` already carries a MySQL driver and a Pulsar
  client for the test environment and adapters, so counting `require` lines
  would fail on a healthy tree and pass on a broken one.
  Its corollary: adapters and tests may take dependencies freely, but
  nothing they import may become reachable from the root package.
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
├── internal/canaljson/ canal-json decoder — shared by all transports;
│                    testdata/ holds fixtures captured from a real TiCDC
├── internal/testenv/ integration-tier containers: one Pulsar, one TiDB
├── pulsar/          Pulsar EventSource adapter — the only package
│                    that imports a Pulsar client
└── test/cdcstack/   full TiDB + TiCDC + Pulsar stack for running the library
                     locally against real CDC; also how fixtures are captured
```

## Testing tiers

Two, split by what they prove:

- **Unit** (`go test ./...`) — no Docker. Includes the fixture-shape check in
  `internal/canaljson`.
- **Integration** (`-tags=integration`) — each test starts the containers it
  needs through `internal/testenv` and tears them down after. There is no stack
  to bring up first, so a skipped tier cannot masquerade as a passing one.

The integration tier deliberately does *not* run TiCDC. cdcfresh consumes bytes
from a topic and cannot tell a live changefeed from a replay, so tests publish
the committed canal-json fixtures instead of spending minutes regenerating
payloads that already exist. Containers are built from the official images
directly (`testcontainers.GenericContainer` with an explicit request), not from
wrapper modules.

`test/cdcstack` runs the whole pipeline locally for hands-on work, and is how
fixtures get recaptured when a TiDB release might have moved the wire format.

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

Unit tier and lint must be green. CI runs both tiers on every push and pull
request — the integration tier starts its own containers, so it needs no
scheduled job and no stack maintained for it.
