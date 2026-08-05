package cdcfresh

import "sync/atomic"

type stats struct {
	eventsReceived atomic.Uint64
	keysMarked     atomic.Uint64
	rebuildsOK     atomic.Uint64
	rebuildsFailed atomic.Uint64
	reconciles     atomic.Uint64
}

// Stats is a point-in-time snapshot of the loop's counters and gauges.
// Publish it however you like — expvar.Publish or a Prometheus collector
// are a few lines each; the library depends on neither.
type Stats struct {
	EventsReceived uint64
	KeysMarked     uint64
	RebuildsOK     uint64
	RebuildsFailed uint64
	Reconciles     uint64
	DirtyKeys      int
	PoisonedKeys   int
}

// Stats returns a consistent snapshot; safe to call from any goroutine.
func (r *Refresher) Stats() Stats {
	r.mu.Lock()
	dirty, poisoned := r.dirty.counts()
	r.mu.Unlock()
	return Stats{
		EventsReceived: r.stats.eventsReceived.Load(),
		KeysMarked:     r.stats.keysMarked.Load(),
		RebuildsOK:     r.stats.rebuildsOK.Load(),
		RebuildsFailed: r.stats.rebuildsFailed.Load(),
		Reconciles:     r.stats.reconciles.Load(),
		DirtyKeys:      dirty,
		PoisonedKeys:   poisoned,
	}
}
