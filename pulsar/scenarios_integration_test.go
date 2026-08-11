//go:build integration

package pulsar_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// recvOr receives from c, failing this test after 60s rather than blocking
// until the package-wide test timeout fires. A bare receive on a channel a
// regression never closes costs every sibling test its result and reports the
// failure as a goroutine dump; a named failure costs only this one.
func recvOr[T any](t *testing.T, c <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(60 * time.Second):
		t.Fatalf("timed out after 60s waiting for %s", what)
		var zero T
		return zero
	}
}

// stopRun cancels a Refresher and waits for Run to return. Every test defers
// it so no library goroutine outlives its test: a worker still unwinding after
// the test function has returned can call OnError, and a t.Logf on a completed
// *testing.T panics the whole package binary.
//
// It reports with t.Errorf, never t.Fatalf, and that is load-bearing rather
// than stylistic. It runs from a defer, and twice from inside a sync.OnceFunc
// — whose recover-and-repanic wrapper turns the runtime.Goexit that t.Fatalf
// performs into panic(nil), i.e. a *runtime.PanicNilError that takes down the
// binary. A Fatal here would re-enter the very failure this function prevents.
//
// Run returns in about a second in practice; the bound is generous, not tuned.
// It is kept well under recvOr's so that five simultaneously failing tests
// still finish inside the 10-minute package timeout, rather than trading named
// failures for the goroutine dump that timeout produces.
func stopRun(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Errorf("Run did not return within 15s of cancellation")
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
	defer src1.Close() // Close is idempotent; the explicit close below still leads

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
	stop1 := sync.OnceFunc(func() { stopRun(t, cancel1, done1) })
	defer stop1()

	waitFor(t, "all three events to be received and acked", func() bool {
		return r1.Stats().EventsReceived == 3
	})
	recvOr(t, blocked, "a rebuild to start") // it is in flight and will never finish

	stop1() // the crash
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
	done2 := make(chan error, 1)
	go func() { done2 <- r2.Run(ctx2) }()
	defer stopRun(t, cancel2, done2)

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
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer stopRun(t, cancel, done)

	waitFor(t, "every backlogged scope to be rebuilt", func() bool {
		return rec.count() == len(devices)
	})
}

// TestPoisonedKeyIsHealedByReconcile runs the quarantine lifecycle against a
// real broker; it is covered elsewhere only with fakes. It is also the
// evidence for closing the Mark(Key) question: a sweep already re-admits a
// key that no event will ever dirty again, which a bare Mark would not.
//
// PoisonedKeys == 0 on its own would prove nothing: markReconcile clears the
// poisoned flag at the start of every sweep, before the retry rebuild runs, so
// the gauge legitimately reads 0 for a few tens of milliseconds of each
// one-second cycle even while the rebuild fails permanently. Healing is only
// established by pairing it with a rebuild that actually succeeded, which is
// why the final wait asserts both in one condition rather than in sequence.
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
	setFailing := func(v bool) { mu.Lock(); failing = v; mu.Unlock() }

	// The refusals are worth seeing when this test fails, but OnError runs on a
	// library goroutine, and t.Logf from one that outlives the test panics the
	// whole package binary. stopRun makes that unreachable on every path except
	// a Run that never returns at all — so buffer here and log after the drain,
	// leaving no route from a library goroutine to *testing.T.
	var errsSeen []string

	r, err := cdcfresh.New(
		cdcfresh.Source(src),
		cdcfresh.Scope(deviceScope),
		cdcfresh.Rebuild(func(_ context.Context, k cdcfresh.Key) error {
			mu.Lock()
			defer mu.Unlock()
			if failing {
				return errors.New("rebuild refused on purpose")
			}
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
		cdcfresh.OnError(func(err error) {
			mu.Lock()
			defer mu.Unlock()
			errsSeen = append(errsSeen, err.Error())
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	// Registered before stopRun so LIFO drains Run first: by the time this
	// reads errsSeen, every goroutine that could append to it has exited.
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range errsSeen {
			t.Logf("%s", e) // the library's messages already carry a "cdcfresh:" prefix
		}
	}()
	defer stopRun(t, cancel, done)

	waitFor(t, "the key to be quarantined after repeated failures", func() bool {
		return r.Stats().PoisonedKeys == 1
	})

	// Only a sweep can revive it now; no further events will arrive.
	setFailing(false)

	// One condition, deliberately: the cleared gauge means "healed" only in the
	// company of a rebuild that succeeded. See the doc comment above.
	waitFor(t, "a reconcile sweep to re-admit the key and clear the quarantine", func() bool {
		s := r.Stats()
		return s.RebuildsOK >= 1 && s.PoisonedKeys == 0
	})
}

// TestBurstOfChangesCollapsesToOneRebuildPerScope measures the library's
// whole value: many changes to few scopes must become few rebuilds. 500
// changes across five scopes collapse to about one rebuild per scope.
//
// What drives the collapse is *consumption* pacing, not production pacing:
// the whole burst is produced and persisted before a consumer exists, and
// coalescing runs off the timestamp receiveLoop takes when it marks a key. So
// the debounce chain for a scope stays alive only because the consumer drains
// same-device messages faster than the 500ms Coalesce window — a property of
// broker throughput, which nothing here bounds.
//
// That is why the bound is a small multiple of the scope count rather than
// exactly one per scope: on a slow drain, MaxWait fires on its own schedule
// (firstDirty+5s, independent of the debounce chain) and each scope flushes
// once per elapsed MaxWait. A couple of those are expected and fine. Anything
// beyond len(devices)*4 means either that coalescing is no longer collapsing
// the burst, or that the drain ran past four MaxWait windows on a very slow
// machine — the logged count says which. Observed here: 5, the floor.
//
// What this does NOT exercise: multi-window behaviour deliberately. Proving
// that a debounce chain eventually flushes mid-burst — the case MaxWait exists
// for — needs spaced-out production and is a different test.
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
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer stopRun(t, cancel, done)

	waitFor(t, "all events to be consumed", func() bool {
		return r.Stats().EventsReceived == totalEvents
	})
	waitFor(t, "every scope to be rebuilt at least once", func() bool {
		return rec.count() == len(devices)
	})

	// Let the last coalesce window close so late rebuilds are counted.
	time.Sleep(2 * time.Second)

	rebuilds := rec.total()
	if limit := len(devices) * 4; rebuilds > limit {
		t.Errorf("%d rebuilds for %d events across %d scopes (limit %d) — coalescing is not collapsing the burst",
			rebuilds, totalEvents, len(devices), limit)
	}
	t.Logf("coalesced %d events across %d scopes into %d rebuilds", totalEvents, len(devices), rebuilds)
}

// TestShutdownMidRebuildReturnsCleanly cancels while a rebuild is in flight.
// Run must return promptly, and the surrendered rebuild must not be reported
// as a failure: a shutdown is not an error, and emitting one would page
// somebody every deploy.
//
// This asserts return rather than the absence of leaked goroutines. Run's own
// WaitGroup gates its return on every loop having exited, so returning is the
// same property; detecting leaks directly would mean adding goleak for one
// assertion.
func TestShutdownMidRebuildReturnsCleanly(t *testing.T) {
	p := testenv.SharedPulsar(t)
	topic := p.Topic(t)

	p.Produce(t, topic, canalInsert("dev-a"))

	src, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription("shutdown"))
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	defer src.Close()

	var mu sync.Mutex
	var errsSeen []string
	entered := make(chan struct{})
	var once sync.Once

	r, err := cdcfresh.New(
		cdcfresh.Source(src),
		cdcfresh.Scope(deviceScope),
		cdcfresh.Rebuild(func(ctx context.Context, _ cdcfresh.Key) error {
			once.Do(func() { close(entered) })
			<-ctx.Done() // still running when the shutdown lands
			return ctx.Err()
		}),
		cdcfresh.Coalesce(20*time.Millisecond),
		cdcfresh.MaxWait(200*time.Millisecond),
		cdcfresh.OnError(func(err error) {
			mu.Lock()
			defer mu.Unlock()
			errsSeen = append(errsSeen, err.Error())
		}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	// Deferred as well as called inline: on any failure path above the inline
	// call, Run must still be drained before the test completes.
	stop := sync.OnceFunc(func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned %v, want context.Canceled", err)
			}
		case <-time.After(30 * time.Second):
			t.Errorf("Run did not return within 30s of cancellation")
		}
	})
	defer stop()

	recvOr(t, entered, "the rebuild to start")
	stop()

	// Safe to read only because stop() returned: onError is reached solely from
	// goroutines inside Run's WaitGroup.
	mu.Lock()
	defer mu.Unlock()
	for _, e := range errsSeen {
		if strings.Contains(e, "rebuild") || strings.Contains(e, "poisoned") {
			t.Errorf("shutdown reported %q; a surrendered rebuild is not a failure", e)
		}
	}
}
