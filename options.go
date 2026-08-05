package cdcfresh

import "time"

// Option configures a Refresher. Source, Scope, and Rebuild are required;
// New reports every missing one in a single error.
type Option func(*config)

type config struct {
	source         EventSource
	scope          ScopeFunc
	rebuild        RebuildFunc
	coalesce       time.Duration
	maxWait        time.Duration
	workers        int
	backoffBase    time.Duration
	backoffCap     time.Duration
	poisonAfter    int
	onError        func(error)
	reconcileEvery time.Duration
	enumerate      EnumerateFunc
}

func defaults() config {
	return config{
		coalesce:    5 * time.Second,
		maxWait:     30 * time.Second,
		workers:     4,
		backoffBase: time.Second,
		backoffCap:  5 * time.Minute,
		poisonAfter: 10,
		onError:     func(error) {},
	}
}

// Source sets the event source (required).
func Source(s EventSource) Option { return func(c *config) { c.source = s } }

// Scope sets the event→keys mapping (required).
func Scope(f ScopeFunc) Option { return func(c *config) { c.scope = f } }

// Rebuild sets the per-key rebuild callback (required).
func Rebuild(f RebuildFunc) Option { return func(c *config) { c.rebuild = f } }

// Coalesce sets the per-key quiet period before a dirty key fires.
func Coalesce(d time.Duration) Option { return func(c *config) { c.coalesce = d } }

// MaxWait caps how long a continuously-dirty key may wait before firing.
func MaxWait(d time.Duration) Option { return func(c *config) { c.maxWait = d } }

// Workers bounds concurrent rebuilds.
func Workers(n int) Option { return func(c *config) { c.workers = n } }

// Backoff sets the retry schedule after rebuild failures: base doubles per
// consecutive failure, capped at max, jittered.
func Backoff(base, max time.Duration) Option {
	return func(c *config) { c.backoffBase, c.backoffCap = base, max }
}

// PoisonAfter quarantines a key after n consecutive rebuild failures; only
// a reconcile sweep re-admits it.
func PoisonAfter(n int) Option { return func(c *config) { c.poisonAfter = n } }

// OnError receives decode, rebuild, poison, and reconcile errors.
func OnError(f func(error)) Option { return func(c *config) { c.onError = f } }

// Reconcile schedules a healing sweep: enumerate lists the live key
// universe; every key flows through the normal pipeline. One sweep runs at
// Run start, then every interval.
func Reconcile(every time.Duration, enumerate EnumerateFunc) Option {
	return func(c *config) { c.reconcileEvery, c.enumerate = every, enumerate }
}
