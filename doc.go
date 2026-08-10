// Package cdcfresh keeps derived tables fresh from change-data-capture
// events: CDC events are the doorbell, your SQL is the recipe, cdcfresh is
// the orchestration in between.
//
// # Model
//
// A Refresher runs one loop: consume decoded CDC events from a Source,
// extract the dirty scope keys each event implies (your Scope function),
// coalesce repeated dirties per key with a debounce-plus-max-wait cap, and
// hand each ready key to your Rebuild callback, which recomputes that scope
// from the source tables. Events never carry decoded column data into your
// SQL — they only say which key is dirty. That is what makes duplicate
// events harmless (Rebuild just runs again) and drift impossible: the
// derived table is always recomputed from truth, never patched in place.
//
// # Usage
//
// Construct a Refresher with New and run it until ctx is cancelled:
//
//	refresher, err := cdcfresh.New(
//		cdcfresh.Source(mySource), // an EventSource — e.g. a Pulsar adapter
//		cdcfresh.Scope(func(ev cdcfresh.RowEvent) []cdcfresh.Key {
//			return []cdcfresh.Key{cdcfresh.Key(ev.Database + "." + ev.Table)}
//		}),
//		cdcfresh.Rebuild(func(ctx context.Context, k cdcfresh.Key) error {
//			return rebuildScope(ctx, k) // your idempotent SQL, scoped to k
//		}),
//		cdcfresh.Coalesce(5*time.Second),
//		cdcfresh.MaxWait(30*time.Second),
//		cdcfresh.Workers(4),
//		cdcfresh.OnError(func(err error) { log.Println(err) }),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	err = refresher.Run(ctx) // blocks until ctx is done or the source fails
//
// Source, Scope, and Rebuild are required; New reports every missing or
// invalid option in a single error rather than failing on the first one.
// Reconcile, Backoff, and PoisonAfter round out the tuning surface — see
// Failures and Reconcile below.
//
// # Guarantees
//
// cdcfresh is at-least-once, not exactly-once: Scope and Rebuild must both
// be idempotent, because retries, redelivery, and the reconcile sweep all
// invoke them more than once for the same event or key. Coalescing is a
// per-key debounce with a max-wait cap — a key fires once it has been quiet
// for Coalesce or dirty for MaxWait, whichever comes first — never a fixed
// window. At most one rebuild is ever in flight per key; an event that
// arrives while a key's rebuild is running re-dirties it, and the key
// re-enters the queue once that rebuild finishes, so no event is ever lost
// to a race with an in-progress rebuild.
//
// A source event is acknowledged as soon as its keys are enqueued, not
// after the rebuild that eventually processes them completes. A crash
// between those two points loses only the in-memory dirty set — the
// reconcile sweep (below) heals it. This is what keeps the library
// stateless: the broker's subscription cursor is the only durable position,
// and cdcfresh never needs one of its own.
//
// # Failures
//
// A failed rebuild retries with exponential backoff and jitter (see
// Backoff), and one failing key never blocks any other key's progress.
// After PoisonAfter consecutive failures a key is quarantined: removed from
// the retry loop, reported once through OnError, and counted in
// Stats.PoisonedKeys. Only a reconcile sweep re-admits a poisoned key, and a
// success there resets its failure count.
//
// # Reconcile
//
// Reconcile(every, enumerate) adds a periodic healing sweep: enumerate
// lists the live key universe, and every key it returns flows through the
// normal pipeline — the same debounce, single-flight, and poison-healing
// logic as CDC-origin keys, with no separate code path. One sweep runs
// immediately when Run starts, then again every interval. The startup
// sweep is what heals a failover takeover: a standby that resumes from the
// broker's last acked position picks up any key that was enqueued but not
// yet rebuilt on the instance it replaced.
//
// # Deployment
//
// cdcfresh assumes a single active consumer per Refresher — for a Pulsar
// source, a failover subscription, with standbys running the same binary
// idle. All state (the dirty set, backoff timers, the poison list) lives in
// memory and dies with the process; a takeover is healed by broker
// redelivery plus the startup reconcile sweep, not by any coordination
// cdcfresh performs itself. A Refresher is single-use: Run returns an error
// immediately if called a second time.
//
// # Observability
//
// Stats returns a point-in-time snapshot of the loop's counters (events
// received/skipped, keys marked, rebuilds ok/failed, reconcile sweeps) and
// gauges (dirty keys, poisoned keys). Counters and gauges are sampled
// independently, so the snapshot is not a single atomic cut across every
// field. cdcfresh has no metrics dependency of its own — publishing a Stats
// snapshot through expvar or a Prometheus collector is a few lines on the
// caller's side.
package cdcfresh
