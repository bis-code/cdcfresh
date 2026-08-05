package cdcfresh

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSource delivers queued events then blocks until ctx is done.
type fakeSource struct {
	mu    sync.Mutex
	queue []Event
}

func (f *fakeSource) Receive(ctx context.Context) (Event, error) {
	f.mu.Lock()
	if len(f.queue) > 0 {
		ev := f.queue[0]
		f.queue = f.queue[1:]
		f.mu.Unlock()
		return ev, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return Event{}, ctx.Err()
}

func rowFor(key string) RowEvent {
	return RowEvent{Database: "db", Table: "t", Type: Update, Data: map[string]any{"k": key}}
}

func keyScope(ev RowEvent) []Key { return []Key{Key(ev.Data["k"].(string))} }

func TestRunCoalescesAcksAndRebuilds(t *testing.T) {
	var acks, rebuilds atomic.Int64
	var gotKey atomic.Value
	src := &fakeSource{}
	for i := 0; i < 3; i++ { // 3 events, same scope → 1 rebuild
		src.queue = append(src.queue, Event{Row: rowFor("a"), Ack: func() { acks.Add(1) }})
	}
	r, err := New(
		Source(src), Scope(keyScope),
		Rebuild(func(_ context.Context, k Key) error {
			gotKey.Store(k)
			rebuilds.Add(1)
			return nil
		}),
		Coalesce(20*time.Millisecond), MaxWait(200*time.Millisecond), Workers(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for rebuilds.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("no rebuild within 2s")
		case <-time.After(5 * time.Millisecond):
		}
	}
	time.Sleep(100 * time.Millisecond) // would catch a spurious 2nd rebuild
	if n := rebuilds.Load(); n != 1 {
		t.Errorf("want exactly 1 coalesced rebuild, got %d", n)
	}
	if k := gotKey.Load(); k != Key("a") {
		t.Errorf("rebuild key = %v, want a", k)
	}
	if n := acks.Load(); n != 3 { // D6: every event acked on enqueue
		t.Errorf("want 3 acks, got %d", n)
	}
	if s := r.Stats(); s.EventsReceived != 3 || s.RebuildsOK != 1 {
		t.Errorf("stats wrong: %+v", s)
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunSurfacesFailuresAndPoison(t *testing.T) {
	var mu sync.Mutex
	var errs []string
	var goodRebuilds atomic.Int64
	src := &fakeSource{queue: []Event{
		{Row: rowFor("bad")}, {Row: rowFor("good")},
	}}
	r, err := New(
		Source(src), Scope(keyScope),
		Rebuild(func(_ context.Context, k Key) error {
			if k == "bad" {
				return errStub
			}
			goodRebuilds.Add(1)
			return nil
		}),
		Coalesce(5*time.Millisecond), MaxWait(50*time.Millisecond),
		Workers(2), PoisonAfter(2), Backoff(5*time.Millisecond, 20*time.Millisecond),
		OnError(func(e error) { mu.Lock(); errs = append(errs, e.Error()); mu.Unlock() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for r.Stats().PoisonedKeys == 0 {
		select {
		case <-deadline:
			t.Fatal("bad key never poisoned")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if goodRebuilds.Load() != 1 {
		t.Errorf("good key must rebuild despite bad key, got %d", goodRebuilds.Load())
	}
	mu.Lock()
	var sawRebuildErr, sawPoison bool
	for _, e := range errs {
		if strings.Contains(e, `rebuild "bad"`) {
			sawRebuildErr = true
		}
		if strings.Contains(e, "poisoned after 2") {
			sawPoison = true
		}
	}
	mu.Unlock()
	if !sawRebuildErr || !sawPoison {
		t.Errorf("OnError missing rebuild/poison reports: %v", errs)
	}
	cancel()
	<-done
}

func TestReconcileStartupSweepAndInterval(t *testing.T) {
	var rebuilds atomic.Int64
	var enumerations atomic.Int64
	src := &fakeSource{} // no CDC traffic at all
	r, err := New(
		Source(src), Scope(keyScope),
		Rebuild(func(_ context.Context, k Key) error { rebuilds.Add(1); return nil }),
		Coalesce(time.Millisecond), MaxWait(10*time.Millisecond),
		Reconcile(50*time.Millisecond, func(context.Context) ([]Key, error) {
			enumerations.Add(1)
			return []Key{"swept"}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for enumerations.Load() < 2 || rebuilds.Load() < 2 { // startup sweep + ≥1 tick
		select {
		case <-deadline:
			t.Fatalf("sweeps=%d rebuilds=%d — want ≥2 each", enumerations.Load(), rebuilds.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if r.Stats().Reconciles < 2 {
		t.Errorf("Reconciles counter = %d, want ≥2", r.Stats().Reconciles)
	}
	cancel()
	<-done
}
