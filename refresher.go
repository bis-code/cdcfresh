package cdcfresh

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Refresher orchestrates the CDC→coalesce→rebuild loop. Create with New,
// start with Run.
type Refresher struct {
	cfg config
	// mu guards dirty. All dirtySet access happens under mu; no user
	// callback (Scope, Rebuild, OnError, enumerate) is ever invoked while
	// holding it — those run either before mu is taken or after it is
	// released.
	mu      sync.Mutex
	dirty   *dirtySet
	nudge   chan struct{}
	stats   stats
	started atomic.Bool
}

// New validates options and builds a Refresher. It reports every missing
// required option, and every option given an invalid value, in one error.
func New(opts ...Option) (*Refresher, error) {
	cfg := defaults()
	for _, o := range opts {
		o(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Refresher{
		cfg: cfg,
		dirty: newDirtySet(cfg.coalesce, cfg.maxWait, cfg.poisonAfter,
			func(n int) time.Duration {
				return retryDelay(cfg.backoffBase, cfg.backoffCap, n, rand.Float64)
			}),
		nudge: make(chan struct{}, 1),
	}, nil
}

// Run consumes the source, coalesces dirty keys, and drives rebuilds until
// ctx is cancelled (returns ctx.Err()) or the source fails (returns the
// wrapped error). A Refresher is single-use: call Run once; a second call
// returns an error without starting anything.
func (r *Refresher) Run(ctx context.Context) error {
	if !r.started.CompareAndSwap(false, true) {
		return errors.New("cdcfresh: Run already called; create a new Refresher")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	work := make(chan Key)
	errc := make(chan error, 1)
	var wg sync.WaitGroup

	for i := 0; i < r.cfg.workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r.workerLoop(ctx, work) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); r.scheduleLoop(ctx, work) }()
	if r.cfg.enumerate != nil {
		wg.Add(1)
		go func() { defer wg.Done(); r.reconcileLoop(ctx) }()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.receiveLoop(ctx); err != nil {
			select {
			case errc <- err:
			default:
			}
			cancel()
		}
	}()

	wg.Wait()
	select {
	case err := <-errc:
		return err
	default:
		return ctx.Err()
	}
}

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
			r.stats.keysMarked.Add(uint64(len(keys)))
			r.mu.Unlock()
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
		for {
			k, ok := r.dirty.pop(now)
			if !ok {
				break
			}
			ready = append(ready, k)
		}
		wakeAt, hasWake := r.dirty.nextWake(now)
		r.mu.Unlock()

		for _, k := range ready {
			select {
			case work <- k:
			case <-ctx.Done():
				return
			}
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
