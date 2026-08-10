# cdcfresh

[![ci](https://github.com/bis-code/cdcfresh/actions/workflows/ci.yml/badge.svg)](https://github.com/bis-code/cdcfresh/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/bis-code/cdcfresh.svg)](https://pkg.go.dev/github.com/bis-code/cdcfresh)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**CDC-powered freshness for derived tables in Go.**

Keep rollup/lookup/read-model tables fresh without SQL triggers, stored
procedures, or cron-scan waste: change-data-capture events are the doorbell,
your own SQL is the recipe, cdcfresh is the orchestration in between.

> Status: v1 is feature-complete — core loop and the TiCDC-on-Pulsar source
> adapter both land and are covered end to end against real infrastructure.
> The API is not yet tagged, so it may still shift. Full model and guarantees
> are in the [package documentation](https://pkg.go.dev/github.com/bis-code/cdcfresh).

```go
src, err := pulsar.Source(
	"pulsar://localhost:6650",
	[]string{"persistent://public/default/cdcfresh"},
	pulsar.WithSubscription("device-totals"),
)
if err != nil {
	return err
}
defer src.Close()

r, err := cdcfresh.New(
	cdcfresh.Source(src),

	// Which derived-table scopes did this change dirty?
	cdcfresh.Scope(func(ev cdcfresh.RowEvent) []cdcfresh.Key {
		device, _ := ev.Data["device"].(string)
		return []cdcfresh.Key{cdcfresh.Key(device)}
	}),

	// Recompute one scope from the source tables. Your SQL, and nothing
	// from the event reaches it — the event only named the scope.
	cdcfresh.Rebuild(func(ctx context.Context, k cdcfresh.Key) error {
		_, err := db.ExecContext(ctx,
			`REPLACE INTO device_totals (device, total)
			 SELECT device, SUM(value) FROM readings WHERE device = ? GROUP BY device`,
			string(k))
		return err
	}),

	cdcfresh.Coalesce(5*time.Second),
	cdcfresh.MaxWait(30*time.Second),
	cdcfresh.Reconcile(time.Hour, allDevices),
)
if err != nil {
	return err
}
return r.Run(ctx)
```

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
| Lag/health counters via a `Stats()` snapshot | Serving reads, HTTP anything |
| Integration tests against real Pulsar and TiDB containers | Multi-instance coordination (single consumer assumed; document failover subscription) |

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
│   ├── canaljson/   canal-json decoder + fixtures captured from a real TiCDC
│   └── testenv/     integration-tier containers: one Pulsar, one TiDB
├── pulsar/          Pulsar EventSource adapter (the only package with a client)
└── test/cdcstack/   full TiDB + TiCDC + Pulsar stack for local development
```

The root package links nothing outside the standard library, and CI proves it
with `go list -deps .`. Adapters and tests take dependencies freely; none of
them may become reachable from the root.

## Testing

```
make test              # every tier
make test-unit         # unit tier only — no Docker
make test-integration  # integration tier — needs Docker
```

Integration tests start the containers they need and stop them again, so
there is nothing to bring up first.

## Observability

`Stats()` returns a plain snapshot struct — no metrics client in the library,
so it costs nothing and dictates nothing. Wire it to whatever you already run:

```go
// expvar, standard library
expvar.Publish("cdcfresh", expvar.Func(func() any { return r.Stats() }))

// Prometheus: set gauges from the same fields on scrape
prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "cdcfresh_dirty_keys"},
	func() float64 { return float64(r.Stats().DirtyKeys) })
```

`DirtyKeys` and `PoisonedKeys` are queue depth; `EventsReceived`,
`EventsSkipped`, `RebuildsOK`, `RebuildsFailed` and `Reconciles` are counters;
`LastEvent` and `LastRebuild` are timestamps for staleness alerting.

## Prior art / positioning

- **Debezium** — the CDC heavyweight, Java, owns capture + delivery; cdcfresh
  starts *after* delivery and stays a library.
- **Materialize / Readyset** — "we run your views" systems; cdcfresh packages
  a pattern instead of hosting your queries.
- **go-mysql, watermill, tiflow internals** — parts of the loop exist; the
  doorbell→dirty-scope→rebuild orchestration as an embeddable package does not.

## License

MIT — see [LICENSE](LICENSE).
