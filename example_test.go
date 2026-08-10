package cdcfresh_test

import (
	"context"
	"expvar"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/bis-code/cdcfresh"
)

// memSource is a stand-in for a real adapter, so this example stays runnable
// without a broker. It blocks once drained, as a real source does between
// events — returning an error there would stop Run.
type memSource struct{ events []cdcfresh.Event }

func (s *memSource) Receive(ctx context.Context) (cdcfresh.Event, error) {
	if len(s.events) == 0 {
		<-ctx.Done()
		return cdcfresh.Event{}, ctx.Err()
	}
	ev := s.events[0]
	s.events = s.events[1:]
	return ev, nil
}

func change(device string) cdcfresh.Event {
	return cdcfresh.Event{Row: cdcfresh.RowEvent{
		Database: "shop",
		Table:    "readings",
		Type:     cdcfresh.Insert,
		Data:     map[string]any{"device": device},
	}}
}

// Example wires a Refresher to a source and watches three row changes across
// two devices collapse into two rebuilds — one per dirty scope, not one per
// event.
func Example() {
	src := &memSource{events: []cdcfresh.Event{
		change("dev-a"), change("dev-a"), change("dev-b"),
	}}

	rebuilt := make(chan cdcfresh.Key, 4)

	r, err := cdcfresh.New(
		cdcfresh.Source(src),

		// Scope names the derived-table scopes a change dirties. It runs
		// inline in the receive loop, so keep it pure and fast.
		cdcfresh.Scope(func(ev cdcfresh.RowEvent) []cdcfresh.Key {
			device, _ := ev.Data["device"].(string)
			return []cdcfresh.Key{cdcfresh.Key("device:" + device)}
		}),

		// Rebuild recomputes one scope from the source tables. It must be
		// idempotent: at-least-once delivery makes repeats routine, and the
		// event is only a doorbell — none of its values belong in this SQL.
		cdcfresh.Rebuild(func(ctx context.Context, k cdcfresh.Key) error {
			rebuilt <- k
			return nil
		}),

		cdcfresh.Coalesce(200*time.Millisecond),
		cdcfresh.MaxWait(2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	// Two scopes were dirtied by three events; the two dev-a changes coalesce.
	keys := []string{string(<-rebuilt), string(<-rebuilt)}
	sort.Strings(keys)
	fmt.Println(keys)

	// Output: [device:dev-a device:dev-b]
}

// ExampleReconcile schedules the healing sweep. Enumerate lists the live key
// universe; every key it returns flows through the normal coalescing
// pipeline, which is also how a quarantined key is re-admitted.
func ExampleReconcile() {
	devices := func(ctx context.Context) ([]cdcfresh.Key, error) {
		// SELECT DISTINCT the scopes that should exist, in practice.
		return []cdcfresh.Key{"device:dev-a", "device:dev-b"}, nil
	}

	_, err := cdcfresh.New(
		cdcfresh.Source(&memSource{}),
		cdcfresh.Scope(func(cdcfresh.RowEvent) []cdcfresh.Key { return nil }),
		cdcfresh.Rebuild(func(context.Context, cdcfresh.Key) error { return nil }),

		// One sweep runs at Run start — which is what heals a failover
		// takeover — and one every interval after it.
		cdcfresh.Reconcile(time.Hour, devices),
	)
	if err != nil {
		log.Fatal(err)
	}
}

// ExampleRefresher_Stats publishes the counters without pulling a metrics
// client into the library. Stats is a plain snapshot struct, so any collector
// can read it: this uses expvar from the standard library, and a Prometheus
// collector would set gauges from the same fields on scrape.
func ExampleRefresher_Stats() {
	r, err := cdcfresh.New(
		cdcfresh.Source(&memSource{}),
		cdcfresh.Scope(func(cdcfresh.RowEvent) []cdcfresh.Key { return nil }),
		cdcfresh.Rebuild(func(context.Context, cdcfresh.Key) error { return nil }),
	)
	if err != nil {
		log.Fatal(err)
	}

	expvar.Publish("cdcfresh", expvar.Func(func() any { return r.Stats() }))

	s := r.Stats()
	fmt.Println(s.EventsReceived, s.RebuildsOK, s.DirtyKeys)

	// Output: 0 0 0
}
