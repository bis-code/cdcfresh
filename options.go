package cdcfresh

import (
	"fmt"
	"strings"
	"time"
)

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

// optionName is the user-facing name of an Option, used in validation
// errors. A named type keeps validate() from accumulating bare strings that
// drift when an Option is renamed.
type optionName string

const (
	optSource      optionName = "Source"
	optScope       optionName = "Scope"
	optRebuild     optionName = "Rebuild"
	optWorkers     optionName = "Workers"
	optCoalesce    optionName = "Coalesce"
	optMaxWait     optionName = "MaxWait"
	optBackoff     optionName = "Backoff"
	optPoisonAfter optionName = "PoisonAfter"
	optReconcile   optionName = "Reconcile"
)

// joinOptionNames renders names as a comma-separated list; strings.Join
// won't take a []optionName directly.
func joinOptionNames(names []optionName) string {
	strs := make([]string, len(names))
	for i, n := range names {
		strs[i] = string(n)
	}
	return strings.Join(strs, ", ")
}

// validate reports every missing required option, and every option given
// an invalid value, in one error.
func (c *config) validate() error {
	var missing []optionName
	if c.source == nil {
		missing = append(missing, optSource)
	}
	if c.scope == nil {
		missing = append(missing, optScope)
	}
	if c.rebuild == nil {
		missing = append(missing, optRebuild)
	}
	var invalid []optionName
	if c.workers < 1 {
		invalid = append(invalid, optWorkers)
	}
	if c.coalesce < 0 {
		invalid = append(invalid, optCoalesce)
	}
	if c.maxWait < 0 {
		invalid = append(invalid, optMaxWait)
	}
	if c.backoffBase < 0 || c.backoffCap < 0 {
		invalid = append(invalid, optBackoff)
	}
	if c.poisonAfter < 1 {
		invalid = append(invalid, optPoisonAfter)
	}
	if c.enumerate != nil && c.reconcileEvery <= 0 {
		invalid = append(invalid, optReconcile)
	}
	var problems []string
	if len(missing) > 0 {
		problems = append(problems, fmt.Sprintf("missing required options: %s", joinOptionNames(missing)))
	}
	if len(invalid) > 0 {
		problems = append(problems, fmt.Sprintf("invalid option values: %s", joinOptionNames(invalid)))
	}
	if len(problems) > 0 {
		return fmt.Errorf("cdcfresh: %s", strings.Join(problems, "; "))
	}
	return nil
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

// OnError receives skipped-event, rebuild, poison, and reconcile errors.
func OnError(f func(error)) Option { return func(c *config) { c.onError = f } }

// Reconcile schedules a healing sweep: enumerate lists the live key
// universe; every key flows through the normal pipeline. One sweep runs at
// Run start, then every interval.
func Reconcile(every time.Duration, enumerate EnumerateFunc) Option {
	return func(c *config) { c.reconcileEvery, c.enumerate = every, enumerate }
}
