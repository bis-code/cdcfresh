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
