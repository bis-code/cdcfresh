package cdcfresh

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (r *Refresher) receiveLoop(ctx context.Context) error {
	for {
		ev, err := r.cfg.source.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, ErrSkip) {
				r.stats.eventsSkipped.Add(1)
				r.cfg.onError(err)
				continue
			}
			return fmt.Errorf("cdcfresh: source: %w", err)
		}
		r.stats.eventsReceived.Add(1)
		now := time.Now()
		r.stats.lastEventNanos.Store(now.UnixNano())
		keys := r.cfg.scope(ev.Row)
		if len(keys) > 0 {
			r.mu.Lock()
			for _, k := range keys {
				r.dirty.mark(k, now)
			}
			r.mu.Unlock()
			// An atomic needs no help from the mutex, and the dirty-set lock
			// is contended by every loop — keep it to what it actually guards.
			r.stats.keysMarked.Add(uint64(len(keys)))
			r.wake()
		}
		if ev.Ack != nil { // D6: ack after enqueue, never after rebuild
			ev.Ack()
		}
	}
}

func (r *Refresher) scheduleLoop(ctx context.Context, work chan<- Key) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		now := time.Now()
		var ready []Key
		r.mu.Lock()
		// Pop at most one batch per worker. Draining everything marks every
		// ready key in-flight while it queues for dispatch, which both
		// inflates the set a shutdown surrenders and turns a burst's
		// re-dirties into redundant re-entries.
		for len(ready) < r.cfg.workers {
			k, ok := r.dirty.pop(now)
			if !ok {
				break
			}
			ready = append(ready, k)
		}
		more := len(ready) == r.cfg.workers
		wakeAt, hasWake := r.dirty.nextWake(now)
		r.mu.Unlock()

		for _, k := range ready {
			select {
			case work <- k:
			case <-ctx.Done():
				return
			}
		}

		// A full batch means there may be more ready right now; go back for
		// it rather than sleeping on the timer.
		if more {
			continue
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		var timerC <-chan time.Time
		if hasWake {
			timer.Reset(time.Until(wakeAt))
			timerC = timer.C
		}
		select {
		case <-ctx.Done():
			return
		case <-r.nudge:
		case <-timerC:
		}
	}
}

func (r *Refresher) workerLoop(ctx context.Context, work <-chan Key) {
	for {
		select {
		case <-ctx.Done():
			return
		case k := <-work:
			err := r.cfg.rebuild(ctx, k)
			if err != nil && ctx.Err() != nil {
				return // shutdown surrenders the whole dirty set (D6/D9)
			}
			r.mu.Lock()
			poisoned := r.dirty.complete(k, err, time.Now())
			r.mu.Unlock()
			if err != nil {
				r.stats.rebuildsFailed.Add(1)
				r.cfg.onError(fmt.Errorf("cdcfresh: rebuild %q: %w", k, err))
				if poisoned {
					r.cfg.onError(fmt.Errorf("cdcfresh: key %q poisoned after %d consecutive failures", k, r.cfg.poisonAfter))
				}
			} else {
				r.stats.rebuildsOK.Add(1)
				r.stats.lastRebuildNanos.Store(time.Now().UnixNano())
			}
			r.wake() // redirty/retry re-entries may need scheduling
		}
	}
}

// wake nudges the scheduler without blocking; a full buffer already
// guarantees a pending wake-up.
func (r *Refresher) wake() {
	select {
	case r.nudge <- struct{}{}:
	default:
	}
}

// reconcileLoop runs one sweep immediately (heals failover takeover, D7/D9)
// then one per interval. Sweep keys flow through the normal pipeline.
func (r *Refresher) reconcileLoop(ctx context.Context) {
	sweep := func() {
		keys, err := r.cfg.enumerate(ctx)
		if err != nil {
			if ctx.Err() == nil {
				r.cfg.onError(fmt.Errorf("cdcfresh: reconcile enumerate: %w", err))
			}
			return
		}
		now := time.Now()
		r.mu.Lock()
		for _, k := range keys {
			r.dirty.markReconcile(k, now)
		}
		r.mu.Unlock()
		r.stats.reconciles.Add(1)
		r.wake()
	}
	sweep()
	ticker := time.NewTicker(r.cfg.reconcileEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}
