package cdcfresh

import (
	"context"
	"errors"
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
