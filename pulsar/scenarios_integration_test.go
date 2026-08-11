//go:build integration

package pulsar_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bis-code/cdcfresh"
	"github.com/bis-code/cdcfresh/internal/testenv"
	"github.com/bis-code/cdcfresh/pulsar"
)

// canalInsert builds a canal-json INSERT naming one device. The shape matches
// the captured fixtures in internal/canaljson/testdata; the decoder's own
// tests pin that shape.
func canalInsert(device string) []byte {
	return []byte(fmt.Sprintf(`{"database":"demo","table":"readings","pkNames":["id"],`+
		`"isDdl":false,"type":"INSERT","es":1,"sql":"","old":null,`+
		`"data":[{"id":"1","device":%q,"value":"1"}]}`, device))
}

// deviceScope maps a row change to the device it dirties.
func deviceScope(ev cdcfresh.RowEvent) []cdcfresh.Key {
	row := ev.Data
	if row == nil {
		row = ev.Old
	}
	device, _ := row["device"].(string)
	if device == "" {
		return nil
	}
	return []cdcfresh.Key{cdcfresh.Key(device)}
}

// waitFor polls cond until it holds, failing the test after 60s. Polling
// rather than sleeping keeps these tests off fixed timing margins.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after 60s waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// recorder collects the keys a rebuild was asked for.
type recorder struct {
	mu   sync.Mutex
	keys map[cdcfresh.Key]int
}

func newRecorder() *recorder { return &recorder{keys: map[cdcfresh.Key]int{}} }

func (r *recorder) add(k cdcfresh.Key) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys[k]++
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

func (r *recorder) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.keys {
		n += c
	}
	return n
}

func (r *recorder) has(k cdcfresh.Key) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.keys[k] > 0
}

// TestRestartResumesWithoutLosingKeys proves the README's central durability
// claim: "a crash loses nothing that redelivery and the reconcile sweep don't
// heal".
//
// The subtlety worth stating, because it is what makes this test meaningful:
// events are acked on enqueue, so anything enqueued but not yet rebuilt when
// the process dies is NOT replayed by the broker — that work is simply gone
// from the in-memory dirty set. The reconcile sweep at Run start is therefore
// the only thing standing behind the claim, and this test asserts exactly
// that by requiring the second run to heal every scope while receiving zero
// events.
func TestRestartResumesWithoutLosingKeys(t *testing.T) {
	p := testenv.SharedPulsar(t)
	topic := p.Topic(t)
	const subscription = "restart"
	devices := []cdcfresh.Key{"dev-a", "dev-b", "dev-c"}

	p.Produce(t, topic,
		canalInsert("dev-a"), canalInsert("dev-b"), canalInsert("dev-c"))

	enumerate := func(context.Context) ([]cdcfresh.Key, error) {
		return devices, nil
	}

	// --- first run: consume and ack everything, rebuild nothing ---
	src1, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription(subscription))
	if err != nil {
		t.Fatalf("Source: %v", err)
	}

	blocked := make(chan struct{})
	var once sync.Once
	r1, err := cdcfresh.New(
		cdcfresh.Source(src1),
		cdcfresh.Scope(deviceScope),
		cdcfresh.Rebuild(func(ctx context.Context, _ cdcfresh.Key) error {
			once.Do(func() { close(blocked) })
			<-ctx.Done() // never completes: this is the work a crash loses
			return ctx.Err()
		}),
		cdcfresh.Coalesce(50*time.Millisecond),
		cdcfresh.MaxWait(500*time.Millisecond),
		cdcfresh.Workers(1),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- r1.Run(ctx1) }()

	waitFor(t, "all three events to be received and acked", func() bool {
		return r1.Stats().EventsReceived == 3
	})
	<-blocked // a rebuild is in flight and will never finish

	cancel1() // the crash
	<-done1
	src1.Close()

	// --- second run: nothing left on the broker, so reconcile must heal ---
	src2, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription(subscription))
	if err != nil {
		t.Fatalf("Source (restart): %v", err)
	}
	defer src2.Close()

	rec := newRecorder()
	r2, err := cdcfresh.New(
		cdcfresh.Source(src2),
		cdcfresh.Scope(deviceScope),
		cdcfresh.Rebuild(func(_ context.Context, k cdcfresh.Key) error {
			rec.add(k)
			return nil
		}),
		cdcfresh.Coalesce(50*time.Millisecond),
		cdcfresh.MaxWait(500*time.Millisecond),
		cdcfresh.Reconcile(time.Hour, enumerate), // one sweep at Run start
	)
	if err != nil {
		t.Fatalf("New (restart): %v", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- r2.Run(ctx2) }()

	waitFor(t, "every scope to be rebuilt after the restart", func() bool {
		for _, d := range devices {
			if !rec.has(d) {
				return false
			}
		}
		return true
	})

	// The healing came from the sweep, not from redelivery: everything was
	// acked before the crash, so a redelivered event here would mean acking
	// is not doing what D6 says it does.
	if got := r2.Stats().EventsReceived; got != 0 {
		t.Errorf("second run received %d events, want 0 — acked work was redelivered", got)
	}

	cancel2()
	if err := <-done2; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Run: %v", err)
	}
}

// TestBacklogIsDrainedOnFirstSubscribe guards the adapter's
// SubscriptionPositionEarliest choice, whose stated reason is that a
// changefeed may have produced before this instance existed. Starting at the
// latest position instead would leave those scopes stale until a sweep.
func TestBacklogIsDrainedOnFirstSubscribe(t *testing.T) {
	p := testenv.SharedPulsar(t)
	topic := p.Topic(t)
	devices := []cdcfresh.Key{"dev-a", "dev-b", "dev-c", "dev-d", "dev-e"}

	// Everything is produced before any consumer exists.
	payloads := make([][]byte, 0, len(devices))
	for _, d := range devices {
		payloads = append(payloads, canalInsert(string(d)))
	}
	p.Produce(t, topic, payloads...)

	src, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription("backlog"))
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	defer src.Close()

	rec := newRecorder()
	// No Reconcile: the backlog is the only thing that can produce a rebuild.
	r, err := cdcfresh.New(
		cdcfresh.Source(src),
		cdcfresh.Scope(deviceScope),
		cdcfresh.Rebuild(func(_ context.Context, k cdcfresh.Key) error {
			rec.add(k)
			return nil
		}),
		cdcfresh.Coalesce(50*time.Millisecond),
		cdcfresh.MaxWait(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitFor(t, "every backlogged scope to be rebuilt", func() bool {
		return rec.count() == len(devices)
	})

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Run: %v", err)
	}
}

// TestPoisonedKeyIsHealedByReconcile runs the quarantine lifecycle against a
// real broker; it is covered elsewhere only with fakes. It is also the
// evidence for closing the Mark(Key) question: a sweep already re-admits a
// key that no event will ever dirty again, which a bare Mark would not.
func TestPoisonedKeyIsHealedByReconcile(t *testing.T) {
	p := testenv.SharedPulsar(t)
	topic := p.Topic(t)
	const device = cdcfresh.Key("dev-a")

	p.Produce(t, topic, canalInsert(string(device)))

	src, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription("poison"))
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	defer src.Close()

	var mu sync.Mutex
	failing := true
	var succeeded bool
	setFailing := func(v bool) { mu.Lock(); failing = v; mu.Unlock() }
	didSucceed := func() bool { mu.Lock(); defer mu.Unlock(); return succeeded }

	r, err := cdcfresh.New(
		cdcfresh.Source(src),
		cdcfresh.Scope(deviceScope),
		cdcfresh.Rebuild(func(_ context.Context, k cdcfresh.Key) error {
			mu.Lock()
			defer mu.Unlock()
			if failing {
				return errors.New("rebuild refused on purpose")
			}
			succeeded = true
			return nil
		}),
		cdcfresh.Coalesce(20*time.Millisecond),
		cdcfresh.MaxWait(200*time.Millisecond),
		cdcfresh.PoisonAfter(2),
		cdcfresh.Backoff(10*time.Millisecond, 50*time.Millisecond),
		// A sweep every second is the only route back for a poisoned key.
		cdcfresh.Reconcile(time.Second, func(context.Context) ([]cdcfresh.Key, error) {
			return []cdcfresh.Key{device}, nil
		}),
		cdcfresh.OnError(func(err error) { t.Logf("cdcfresh: %v", err) }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitFor(t, "the key to be quarantined after repeated failures", func() bool {
		return r.Stats().PoisonedKeys == 1
	})

	// Only a sweep can revive it now; no further events will arrive.
	setFailing(false)

	waitFor(t, "a reconcile sweep to re-admit the poisoned key", didSucceed)

	waitFor(t, "the quarantine to clear", func() bool {
		return r.Stats().PoisonedKeys == 0
	})

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Run: %v", err)
	}
}

// TestBurstOfChangesCollapsesToOneRebuildPerScope measures the library's
// whole value: many changes to few scopes must become few rebuilds. 500
// changes across five scopes, produced back-to-back with no gaps, collapse
// to one rebuild per scope — the debounce chain for each scope keeps
// resetting until production ends, so the whole burst lands inside a single
// coalesce window per key.
//
// What this does NOT exercise: multi-window behaviour. Production is
// round-robin across devices with no delay, so same-device messages arrive
// far under the Coalesce window for the entire run; MaxWait never fires.
// Proving that a debounce chain eventually flushes mid-burst — the case
// MaxWait exists for — needs spaced-out production and is a different test.
// The bound here is deliberately loose — the exact ratio moves with timing,
// and pinning it would manufacture a flake. What matters is catching a
// collapse to one-rebuild-per-event.
func TestBurstOfChangesCollapsesToOneRebuildPerScope(t *testing.T) {
	p := testenv.SharedPulsar(t)
	topic := p.Topic(t)
	devices := []string{"dev-a", "dev-b", "dev-c", "dev-d", "dev-e"}
	const eventsPerDevice = 100
	const totalEvents = eventsPerDevice * 5

	payloads := make([][]byte, 0, totalEvents)
	for i := 0; i < eventsPerDevice; i++ {
		for _, d := range devices {
			payloads = append(payloads, canalInsert(d))
		}
	}
	p.Produce(t, topic, payloads...)

	src, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription("load"))
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	defer src.Close()

	rec := newRecorder()
	r, err := cdcfresh.New(
		cdcfresh.Source(src),
		cdcfresh.Scope(deviceScope),
		cdcfresh.Rebuild(func(_ context.Context, k cdcfresh.Key) error {
			rec.add(k)
			return nil
		}),
		cdcfresh.Coalesce(500*time.Millisecond),
		cdcfresh.MaxWait(5*time.Second),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	waitFor(t, "all events to be consumed", func() bool {
		return r.Stats().EventsReceived == totalEvents
	})
	waitFor(t, "every scope to be rebuilt at least once", func() bool {
		return rec.count() == len(devices)
	})

	// Let the last coalesce window close so late rebuilds are counted.
	time.Sleep(2 * time.Second)

	rebuilds := rec.total()
	if rebuilds >= totalEvents/5 {
		t.Errorf("%d rebuilds for %d events across %d scopes — coalescing is not collapsing the burst",
			rebuilds, totalEvents, len(devices))
	}
	t.Logf("coalesced %d events across %d scopes into %d rebuilds", totalEvents, len(devices), rebuilds)

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Run: %v", err)
	}
}
