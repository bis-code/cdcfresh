package cdcfresh

import (
	"sync/atomic"
	"time"
)

type stats struct {
	eventsReceived   atomic.Uint64
	eventsSkipped    atomic.Uint64
	keysMarked       atomic.Uint64
	rebuildsOK       atomic.Uint64
	rebuildsFailed   atomic.Uint64
	reconciles       atomic.Uint64
	lastEventNanos   atomic.Int64
	lastRebuildNanos atomic.Int64
}

// Stats is a point-in-time snapshot of the loop's counters and gauges.
// Counters and gauges are sampled independently — counters are atomic
// reads, gauges (DirtyKeys, PoisonedKeys) are read under the dirty-set
// mutex — so the snapshot is not a single atomic cut across all fields.
// Publish it however you like — expvar.Publish or a Prometheus collector
// are a few lines each; the library depends on neither.
type Stats struct {
	EventsReceived uint64
	EventsSkipped  uint64
	KeysMarked     uint64
	RebuildsOK     uint64
	RebuildsFailed uint64
	Reconciles     uint64
	DirtyKeys      int
	PoisonedKeys   int
	LastEvent      time.Time // zero if no event has been received yet
	LastRebuild    time.Time // zero if no rebuild has completed yet
}

// Stats returns a snapshot of the loop's counters and gauges; safe to call
// from any goroutine.
func (r *Refresher) Stats() Stats {
	r.mu.Lock()
	dirty, poisoned := r.dirty.counts()
	r.mu.Unlock()
	return Stats{
		EventsReceived: r.stats.eventsReceived.Load(),
		EventsSkipped:  r.stats.eventsSkipped.Load(),
		KeysMarked:     r.stats.keysMarked.Load(),
		RebuildsOK:     r.stats.rebuildsOK.Load(),
		RebuildsFailed: r.stats.rebuildsFailed.Load(),
		Reconciles:     r.stats.reconciles.Load(),
		DirtyKeys:      dirty,
		PoisonedKeys:   poisoned,
		LastEvent:      nanosToTime(r.stats.lastEventNanos.Load()),
		LastRebuild:    nanosToTime(r.stats.lastRebuildNanos.Load()),
	}
}

// nanosToTime converts a unix-nanos timestamp back to time.Time, keeping
// the zero value zero instead of mapping it to the unix epoch.
func nanosToTime(nanos int64) time.Time {
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}
