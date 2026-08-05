package cdcfresh

import (
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
