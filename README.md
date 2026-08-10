# cdcfresh

**CDC-powered freshness for derived tables in Go.**

Keep rollup/lookup/read-model tables fresh without SQL triggers, stored
procedures, or cron-scan waste: change-data-capture events are the doorbell,
your own SQL is the recipe, cdcfresh is the orchestration in between.

> Status: core loop implemented (design record: `docs/design.md`). Pulsar
> source adapter and test harness are in progress; API may still shift.

## The pattern

Databases like TiDB have no triggers, procedures, or event schedulers — and
even where triggers exist, in-transaction delta maintenance drifts and taxes
every write. The robust alternative is a loop that every team keeps
re-implementing by hand:

```
CDC stream ──► extract dirty scope keys ──► coalesce ──► scoped rebuild-from-truth
   (TiCDC)         (your function)          (debounce)      (your SQL, idempotent)
                                                     └──► reconcile sweep (safety net)
```

The events are only a doorbell, never the data: rebuilds always recompute the
affected scope from the source tables, so duplicates are harmless
(at-least-once friendly) and drift is impossible by construction.

## Sketch (target API)

```go
refresher := cdcfresh.New(
    cdcfresh.FromPulsar(client, "cdc-changes", cdcfresh.CanalJSON()), // TiCDC → Pulsar sink
    cdcfresh.Scope(func(ev cdcfresh.RowEvent) []cdcfresh.Key {
        // row event → which derived-table scopes are now dirty
    }),
    cdcfresh.Coalesce(5*time.Second),              // 100 changes to one scope = 1 rebuild
    cdcfresh.Rebuild(func(ctx context.Context, k cdcfresh.Key) error {
        // run YOUR set-based rebuild SQL, limited to scope k
    }),
    cdcfresh.Reconcile(6*time.Hour),               // periodic full rebuild heals anything missed
)
refresher.Run(ctx)
```

## Design principles

- **Ephemeral / stateless** — cdcfresh owns no durable state. The dirty-scope
  queue is in memory; stream position is the broker subscription cursor; a
  crash loses nothing that redelivery + the reconcile sweep don't heal.
  Embeddable in any Go service the way testcontainers is embeddable in any
  test suite: import, configure in a few lines, run.
- **Your SQL stays yours** — the library never generates or manages queries,
  tables, or schemas. It decides *when* and *for which scope* to run what you
  wrote.
- **At-least-once native** — every callback must be (and is documented as)
  idempotent; duplicate events are a non-event.
- **Start small** — v1 is a prototype for exactly one source technology:
  **TiCDC → Pulsar (canal-json)**. The source is behind a tiny interface so
  MySQL binlog / Kafka adapters can come later, but they are explicitly out of
  scope for v1.

## v1 scope

| In | Out (v1) |
|---|---|
| Pulsar consumer for TiCDC canal-json events | Kafka / MySQL-binlog / Postgres sources |
| Scope extraction, per-key coalescing/debounce | Exactly-once guarantees |
| Rebuild invocation with retry + backoff | Generating rebuild SQL |
| Reconcile sweep scheduler | Managing schemas or migrations |
| Lag/health counters (expvar or prometheus) | Serving reads, HTTP anything |
| Throwaway harness: compose/testcontainers with TiDB + TiCDC + Pulsar | Multi-instance coordination (single consumer assumed; document failover subscription) |

## Repository layout

```
cdcfresh/            root package — import "github.com/bis-code/cdcfresh"
├── event.go         public contract: Key, RowEvent, Event, EventSource, ErrSkip
├── options.go       Option constructors + validation
├── refresher.go     Refresher, New, Run
├── loops.go         receive / schedule / worker / reconcile goroutines
├── coalesce.go      dirty-set state machine (pure, explicit clock)
├── backoff.go       retry delay
├── stats.go         atomic counters + Stats snapshot
├── internal/
│   └── canaljson/   [planned] canal-json decoder — shared by all transports
├── pulsar/          [planned] Pulsar EventSource adapter
└── harness/         [planned] TiDB + TiCDC + Pulsar stack for the dev loop and integration tests
```

The root package is stdlib-only; each transport adapter keeps its client
dependency in its own subdirectory.

## Testing

```
make test              # every tier
make test-unit         # unit tier only
make test-integration  # integration tier — needs Docker
```

## Prior art / positioning

- **Debezium** — the CDC heavyweight, Java, owns capture + delivery; cdcfresh
  starts *after* delivery and stays a library.
- **Materialize / Readyset** — "we run your views" systems; cdcfresh packages
  a pattern instead of hosting your queries.
- **go-mysql, watermill, tiflow internals** — parts of the loop exist; the
  doorbell→dirty-scope→rebuild orchestration as an embeddable package does not.

## License

MIT (intended; added with first real code).
