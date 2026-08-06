# cdcfresh v1 design

Design record for v1 (2026-08-05, amended 2026-08-06). Each section carries
**Decision / Rationale / Rejected**. The concept, name, v1 scope, and design
principles in `README.md` are inputs here, not up for revision. Two decisions
(D6 ack policy, D7 reconcile semantics) emerged during design; they are
included because they define the at-least-once boundary of the whole library.

## Pipeline overview

```
Pulsar failover subscription (cdcfresh/pulsar)
      │  canal-json bytes
      ▼
decode → RowEvent          (D3; unparseable → OnError, skip, count)
      │
      ▼
Scope(ev) → []Key          (user callback, D2)
      │
      ▼  ack message       (D6: ack after enqueue, not after rebuild)
dirty-set + debounce       (D4: per-key quiet-period + max-wait)
      │  key becomes "ready"
      ▼
ready FIFO → worker pool   (D4: bounded, single-flight per key)
      │
      ▼
Rebuild(ctx, key)          (user SQL; retry/backoff/poison, D5)

Reconcile(interval, enumerate) ─► enqueues keys through the same pipeline
                                  (D7: on Run() start + every interval)
```

### Invariants (hold everywhere, cited by section)

1. **At most one in-flight rebuild per key** (single-flight).
2. **Dirty-during-rebuild is never lost**: an event arriving for a key whose
   rebuild is running re-dirties the key; it re-enters debounce after the
   in-flight rebuild finishes.
3. **Events are doorbells**: no decoded column value ever reaches user SQL
   through the library; rebuilds recompute from source tables. This is what
   makes every lossy path below (skip on decode error, ack-on-enqueue, crash)
   safe — the reconcile sweep heals anything missed.

## D1 — API shape: functional options

**Decision.** Functional options, exactly as the README sketches:
`cdcfresh.New(opts ...Option) (*Refresher, error)`. `Source`, `Scope`, and
`Rebuild` are required options; `New` fails fast with one error naming every
missing required option. Everything else (`Coalesce`, `MaxWait`, `Reconcile`,
`Workers`, `Backoff`, `OnError`) is a tuning option with a documented default.
`Refresher.Run(ctx) error` blocks until ctx cancellation or fatal error.

**Rationale.** The README's target API already commits to this shape, and it
is the idiomatic Go pattern for a library with 3 required components and a
long tail of optional tuning — options can be added forever without breaking
the signature. Required-as-option is the one known weakness; a fail-fast
`New` error listing all missing options at once neutralizes it.

**Rejected.** *Config struct* — makes required fields visible in godoc, but
zero-values are ambiguous (is `Coalesce: 0` "disable" or "default"?), and
evolving defaults/validation is messier than adding an option. *Builder
chain* — un-idiomatic in Go, verbose, and defers all validation to the
terminal call anyway, which is exactly what `New(...) (_, error)` already does.

## D2 — Key type: opaque string

**Decision.** `type Key string`. The user encodes whatever structure their
scopes have (`"device:123"`, `"tenant:acme/day:2026-08-05"`); the library
never parses it. Documented convention only, no helper types.

**Rationale.** Keys must be comparable (map key for the dirty-set), printable
(logs, metrics, errors), and cheap. String is all three with zero API tax.
The killer case against alternatives: one Refresher may serve scopes of
heterogeneous shapes (a row-level key and a tenant-level key from the same
event stream) — natural with strings, impossible with a single typed key.

**Rejected.** *Generic `K comparable`* — the type parameter infects
`Refresher`, every option, `Scope`, `Rebuild`, and the Source interface, and
metrics/logging need a string form anyway, collapsing the benefit to roughly
a `Stringer` bound. *`struct{Table, ID string}`* — presumes scopes are
table+row, which contradicts "your SQL stays yours"; reconcile-level and
bucket-level scopes don't fit that shape.

## D3 — canal-json decoding: hand-rolled minimal structs

**Decision.** A small hand-written decoder in-repo covering only the doorbell
fields: `database`, `table`, `type` (INSERT/UPDATE/DELETE), `isDdl`, `data`,
`old`, `pkNames`, `ts`/`commitTs`. Unknown fields ignored; DDL and watermark
events dropped by default; an event that fails to decode is surfaced through
`OnError`, counted, and skipped. Golden-file tests use real TiCDC output
captured from the harness (D8).

**Rationale.** cdcfresh never applies event data — worst case for a mis-parse
is a wrong or missed dirty key, which the reconcile sweep heals (invariant 3).
That safety net is what makes a minimal decoder acceptable here when it
wouldn't be in a data-applying CDC consumer. The documented canal-json format
is the contract, pinned by golden files from a real TiCDC.

**Rejected.** *Importing `pingcap/tiflow` decoder machinery* — always current,
but drags the TiCDC server's dependency graph (pingcap/tidb transitive deps,
replace directives) into every consumer of the library. Disqualifying for an
embeddable package; revisit only if canal-json quirks demonstrably outrun the
golden-file tests.

## D4 — Coalescing: per-key debounce with max-wait, single-flight workers

**Decision.** Per-key **debounce with a max-wait cap**: a key becomes ready
when it has been quiet for `Coalesce` (default 5s) *or* has been dirty for
`MaxWait` (default 30s), whichever comes first. Ready keys enter a FIFO drained
by a bounded worker pool (`Workers`, default 4) with single-flight per key
(invariant 1) and re-dirty semantics during rebuild (invariant 2). A hot key
re-enters at the tail after each rebuild, so it cannot starve quiet keys.

**Rationale.** Pure debounce never fires for a continuously-hot scope; a fixed
window double-fires when a burst straddles the window edge and makes quiet
keys wait the full window. Debounce+max-wait subsumes both (set `Coalesce=0`
to get pure windowing) and directly answers the hot-scope fairness question:
FIFO + single-flight + tail re-entry bounds any key's share of the pool to
one worker.

**Rejected.** *Fixed window only* — simpler, but the edge-straddle double
rebuild and the added latency floor for quiet keys are exactly the waste this
library exists to remove. *Pure debounce* — hot-scope starvation is a
correctness bug (a table under constant write load would never refresh).

## D5 — Failure model: per-key backoff, poison quarantine

**Decision.** A failed rebuild schedules a per-key retry with exponential
backoff + jitter (default 1s base, ×2, cap 5min); the key holds its dirty
state and workers move on — one failing key never blocks others. After
`PoisonAfter` consecutive failures (default 10) the key is **quarantined**:
removed from the hot loop, exposed via a `poisoned_keys` counter and a final
`OnError` call, and retried only when the reconcile sweep re-enqueues it
(which resets its failure count on success). No dead-letter queue.

**Rationale.** A key failing 10 straight times under backoff is a bug or a bad
row; hammering it forever burns a worker slot every 5 minutes with no operator
signal beyond log noise. Quarantine converts it into an explicit metric and
hands retry-of-last-resort to reconcile — the mechanism the design already
trusts to heal everything else. Staleness of a poisoned key is bounded by the
reconcile interval, which the operator chose.

**Rejected.** *Fail-fast (stop `Run` on rebuild error)* — one bad scope halts
freshness for every table; unacceptable. *Durable dead-letter (publish poison
keys to a broker topic)* — gives cdcfresh its own state to own and operate,
violating the ephemeral principle for a marginal gain over metric+reconcile.

## D6 — Ack policy: ack on enqueue (emergent decision)

**Decision.** A Pulsar message is acked as soon as its extracted keys are
accepted into the dirty-set — *not* after the corresponding rebuild completes.
A crash between enqueue and rebuild loses only in-memory dirty keys, which the
reconcile sweep heals.

**Rationale.** Coalescing deliberately many-to-ones messages into rebuilds, so
there is no per-message completion event to ack on; holding acks until rebuild
would couple the subscription cursor to rebuild latency, grow unacked backlog
under load, and trigger redelivery storms — for a guarantee the library
doesn't need, because events are doorbells (invariant 3).

**Rejected.** *Ack after rebuild* — approximates effectively-once bookkeeping
the design explicitly does not promise (v1 scope excludes exactly-once), at
the cost of cursor stalls and redelivery amplification. Doorbell + reconcile
makes the stronger coupling pure downside.

**Ephemerality note.** Ack stays ephemeral because the broker owns the only
durable position: Pulsar persists the subscription cursor server-side, so
acking just advances state the broker keeps for every subscriber anyway. The
Source contract therefore includes an ack handle but never a position store.
Kafka maps cleanly (consumer-group offsets are broker-held too); MySQL binlog
does not — no server-side per-consumer cursor exists, so a binlog adapter
would need its own position store. That is *why* binlog is out of v1, not
merely "not done yet".

## D7 — Reconcile semantics: user-enumerated keys + startup sweep (emergent)

**Decision.** `Reconcile(interval, enumerate func(ctx) ([]Key, error))` — the
user enumerates the live key universe; cdcfresh enqueues those keys through
the normal pipeline (debounce dedupes against hot traffic, single-flight and
poison-reset apply automatically). One sweep runs at `Run()` start, then every
interval. Enumeration errors go to `OnError` and the sweep re-arms.

**Rationale.** Only the user knows the key universe, and routing sweep keys
through the ordinary pipeline means one code path, natural dedup against
concurrent CDC traffic, and free poison-key healing. The startup sweep closes
the failover gap in D9: a standby taking over resumes from acked cursor
position, so keys that were enqueued-but-unrebuilt on the dead instance are
recovered immediately rather than after up to one full interval.

**Rejected.** *Opaque "full rebuild" callback separate from the pipeline* —
a second execution path that bypasses coalescing/single-flight and can race
the hot loop on the same scope, violating invariant 1 exactly when the system
is already stressed.

## D8 — Observability: zero-dep counters (emergent)

**Decision.** Core keeps atomic counters (events decoded/skipped, keys
enqueued, rebuilds ok/failed, poisoned keys, queue depth, last event/rebuild
timestamps) exposed via `Refresher.Stats() Stats` — a plain struct snapshot.
No metrics dependency in core; expvar or Prometheus publication is a
few-line user-side adapter, shown in docs.

**Rationale.** README says "expvar or prometheus"; the answer is *neither in
core*. A snapshot struct is trivially adaptable to both and keeps the root
package stdlib-only (D10).

**Rejected.** *Prometheus client in core* — a heavyweight, version-sensitive
dep forced on every consumer. *expvar in core* — cheap but publishes global
process state as a side effect; better as the documented example adapter.

## D9 — Multi-instance story: failover subscription is enough for v1

**Decision.** Document exactly one supported topology: a single active
consumer via a Pulsar **failover** subscription; standbys run the same binary.
In-memory state (dirty-set, backoff, poison list) dies with the instance;
takeover is healed by unacked redelivery plus the D7 startup sweep. Per-key
routing across instances is explicitly out of v1.

**Rationale.** The bottleneck is the database executing rebuild SQL, not the
consumer — a single process with a bounded worker pool saturates the DB long
before it saturates a Pulsar subscription. Failover gives HA without any
coordination machinery, and the startup sweep bounds the takeover staleness
window to roughly one sweep.

**Rejected.** *Key_Shared subscription with per-key routing* — horizontal
scaling for a bottleneck that hasn't been observed; it forces per-key state
to follow key ownership across instances and reintroduces the coordination
complexity the ephemeral principle exists to avoid. Revisit with evidence
(a real deployment where one instance's worker pool can't keep up).

## D10 — Harness: compose for the dev loop, testcontainers-go for CI (both)

**Decision.** Both, layered. `harness/docker-compose.yml` (TiDB + TiCDC +
Pulsar standalone + changefeed bootstrap) for interactive development —
brought up once, iterated against. One end-to-end test drives the full loop
(SQL write → TiCDC → Pulsar → rebuild observed) via testcontainers-go behind
`//go:build e2e`, so plain `go test ./...` runs only unit tests against a fake
Source (sub-second). Realistic e2e timing: minutes cold (image pulls), under
~90s warm — acceptable for a separate CI job, never for the default target.
The e2e run doubles as the golden-file capture source for D3.

**Rationale.** The two tools serve different loops: compose amortizes startup
for humans; testcontainers makes CI self-contained and can even reuse the
compose file via its compose module. The "how fast can `go test ./...` demo
the loop" answer is: it doesn't — the default target proves the orchestration
logic fast; proving the real wiring is an opt-in tagged job.

**Rejected.** *testcontainers-only* — every human iteration pays full-stack
startup; hostile dev loop. *compose-only* — CI has nothing self-verifying;
"works on my machine" for the exact date/stat/portability class of bug that
Linux-only CI exists to catch.

## D11 — Repo bootstrap and package layout

**Decision.**
- `go.mod`: `module github.com/bis-code/cdcfresh`, `go 1.25` (library
  minimum; CI tests on latest stable).
- Layout: root package `cdcfresh` is **stdlib-only** (core loop, options,
  decode structs, Stats). The Pulsar consumer lives in subpackage
  `cdcfresh/pulsar`, the only place `apache/pulsar-client-go` is imported —
  the README sketch's `cdcfresh.FromPulsar(...)` becomes
  `pulsar.Source(...)`, an intra-scope refinement of the target API (D1's
  question owns API shape).
- MIT `LICENSE`, copyright "2026 bis-code" (GitHub identity, consistent with
  the public-repo authorship).
- CI: GitHub Actions on `ubuntu-latest` — gofmt check, `go vet`, `go test`
  (unit only); the e2e job from D10 is added when the harness lands.
- `.gitignore`: Go binaries/coverage artifacts, editor and OS noise.
- Roadmap: `now`/`next` labels + seed issues on GitHub after the repo is
  created (queued as a manual step — no remote exists yet).

**Rationale.** Dependency isolation is the standard Go courtesy pattern: the
build graph only links imported packages, so future non-Pulsar users never
compile the Pulsar client. `go 1.25` is a conservative library minimum for
mid-2026 without chasing the bleeding edge. Linux-only CI mirrors both the
deployment ecosystem and the harness's container requirements.

**Rejected.** *Single flat package for v1* — simpler today, but moving
`FromPulsar` out of the root package later is an API break; starting isolated
costs one directory now. *Pinning CI to the newest Go only* — a library
should prove its stated minimum; `setup-go` runs stable and the `go.mod`
directive enforces the floor.

## Amendments — 2026-08-06 (post-implementation architecture review)

- **A1 (amends D3/D11) — decoder location: `internal/canal`.** The canal-json
  decode structs live in `internal/canal` (importing the root package for
  `RowEvent`/`EventType`), not in the root package: unexported-in-root is
  unreachable from `cdcfresh/pulsar`, and exported-in-root would make a
  TiCDC-format parser permanent public API of the format-neutral package.
  `internal/` is importable module-wide, invisible to users, and promotable
  to a public `cdcfresh/canal` later without a break. DDL/watermark
  drop-by-default becomes a canal/pulsar option accordingly.
- **A2 (amends D3/D8) — non-fatal skip lane: `ErrSkip`.** `EventSource`
  errors were originally all fatal, leaving D3's "decode failures are
  surfaced, counted, skipped" with no route into the core. An exported
  sentinel `cdcfresh.ErrSkip` closes it: the receive loop treats an error
  wrapping `ErrSkip` as non-fatal (counts `Stats.EventsSkipped`, calls
  `OnError`, continues); the adapter wraps undecodable-event errors with it
  and acks the dropped message itself, since it holds the ack handle.
- **A3 (amends D1) — `New` validates values, not just presence** (workers,
  durations, poison threshold, reconcile interval), and `Run` is enforced
  single-use via an atomic guard — both landed with the core loop.

## Out of scope (unchanged from README)

Kafka/binlog/Postgres sources, exactly-once, SQL generation, schema
management, HTTP surface, multi-instance coordination beyond D9.
