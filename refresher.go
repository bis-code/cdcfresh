package cdcfresh

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

// Refresher orchestrates the CDC→coalesce→rebuild loop. Create with New,
// start with Run.
type Refresher struct {
	cfg   config
	mu    sync.Mutex // guards dirty
	dirty *dirtySet
	nudge chan struct{}
	stats stats
}

// New validates options and builds a Refresher. It reports every missing
// required option in one error.
func New(opts ...Option) (*Refresher, error) {
	cfg := defaults()
	for _, o := range opts {
		o(&cfg)
	}
	var missing []string
	if cfg.source == nil {
		missing = append(missing, "Source")
	}
	if cfg.scope == nil {
		missing = append(missing, "Scope")
	}
	if cfg.rebuild == nil {
		missing = append(missing, "Rebuild")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("cdcfresh: missing required options: %s", strings.Join(missing, ", "))
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
// wrapped error). Call it once per Refresher.
func (r *Refresher) Run(ctx context.Context) error {
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
			return fmt.Errorf("cdcfresh: source: %w", err)
		}
		r.stats.eventsReceived.Add(1)
		keys := r.cfg.scope(ev.Row)
		if len(keys) > 0 {
			now := time.Now()
			r.mu.Lock()
			for _, k := range keys {
				r.dirty.mark(k, now)
			}
			r.mu.Unlock()
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

func (r *Refresher) reconcileLoop(ctx context.Context) { <-ctx.Done() }
