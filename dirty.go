package cdcfresh

import "time"

// dirtySet is the coalescing heart: a pure state machine over dirty keys.
// It is unsynchronized and never reads the clock — every operation takes
// an explicit now. Refresher serializes access behind one mutex.
type dirtySet struct {
	coalesce    time.Duration
	maxWait     time.Duration
	poisonAfter int
	backoff     func(attempt int) time.Duration

	entries map[Key]*keyState
	order   []Key // FIFO of keys awaiting pop (excludes inFlight and poisoned)
}

type keyState struct {
	firstDirty time.Time
	lastEvent  time.Time
	inFlight   bool
	redirty    bool
	poisoned   bool
	fails      int
	retryAt    time.Time
}

func newDirtySet(coalesce, maxWait time.Duration, poisonAfter int, backoff func(int) time.Duration) *dirtySet {
	return &dirtySet{
		coalesce:    coalesce,
		maxWait:     maxWait,
		poisonAfter: poisonAfter,
		backoff:     backoff,
		entries:     make(map[Key]*keyState),
	}
}

// mark records a CDC-origin dirty signal. Poisoned keys ignore it (D5);
// keys mid-rebuild remember it (invariant 2).
func (d *dirtySet) mark(k Key, now time.Time) {
	st, ok := d.entries[k]
	if !ok {
		d.entries[k] = &keyState{firstDirty: now, lastEvent: now}
		d.order = append(d.order, k)
		return
	}
	switch {
	case st.poisoned:
	case st.inFlight:
		st.redirty = true
	default:
		st.lastEvent = now
	}
}

// pop returns the first ready key in FIFO order and marks it in-flight.
func (d *dirtySet) pop(now time.Time) (Key, bool) {
	for i, k := range d.order {
		st := d.entries[k]
		if !d.ready(st, now) {
			continue
		}
		d.order = append(d.order[:i:i], d.order[i+1:]...)
		st.inFlight = true
		st.redirty = false
		return k, true
	}
	return "", false
}

func (d *dirtySet) ready(st *keyState, now time.Time) bool {
	if st.fails > 0 {
		return !now.Before(st.retryAt)
	}
	return !now.Before(st.lastEvent.Add(d.coalesce)) ||
		!now.Before(st.firstDirty.Add(d.maxWait))
}

// complete records a finished rebuild. Success clears failure history and
// either removes the key or, if it was re-dirtied mid-rebuild, re-enters
// it at the FIFO tail as fresh-dirty at now (invariant 2).
func (d *dirtySet) complete(k Key, rebuildErr error, now time.Time) (poisoned bool) {
	st, ok := d.entries[k]
	if !ok {
		return false
	}
	st.inFlight = false
	if rebuildErr == nil {
		st.fails = 0
		if st.redirty {
			st.redirty = false
			st.firstDirty, st.lastEvent = now, now
			d.order = append(d.order, k)
		} else {
			delete(d.entries, k)
		}
		return false
	}
	// failure path: Task 5
	return false
}
