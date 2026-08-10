package cdcfresh

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// seqSource replays a fixed sequence of (Event, error) responses in order,
// then blocks until ctx is done.
type seqSource struct {
	mu  sync.Mutex
	seq []func() (Event, error)
}

func (s *seqSource) Receive(ctx context.Context) (Event, error) {
	s.mu.Lock()
	if len(s.seq) > 0 {
		fn := s.seq[0]
		s.seq = s.seq[1:]
		s.mu.Unlock()
		return fn()
	}
	s.mu.Unlock()
	<-ctx.Done()
	return Event{}, ctx.Err()
}

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
		Coalesce(200*time.Millisecond), MaxWait(2*time.Second), Workers(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(10 * time.Second)
	for rebuilds.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("no rebuild within 10s")
		case <-time.After(5 * time.Millisecond):
		}
	}
	// Longer than the coalesce window: a spurious second rebuild would have
	// to wait one out, so a shorter sleep could not have caught it.
	time.Sleep(300 * time.Millisecond)
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// hasPoison reads errs under its mutex — the only safe way to read it,
	// since OnError appends to it from a worker goroutine.
	hasPoison := func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range errs {
			if strings.Contains(e, "poisoned after 2") {
				return true
			}
		}
		return false
	}
	// Poll for the poison OnError report directly, not Stats().PoisonedKeys:
	// dirty.complete marks the key poisoned (visible via Stats) before
	// workerLoop's corresponding onError call for it has necessarily run,
	// so gating on the gauge raced the report this assertion depends on.
	deadline := time.After(10 * time.Second)
	for !hasPoison() {
		select {
		case <-deadline:
			t.Fatal("bad key never reported as poisoned via OnError")
		case <-time.After(5 * time.Millisecond):
		}
	}
	// The good key rebuilds independently of the bad one, so it need not have
	// finished by the moment poisoning is reported — poll for it instead of
	// assuming an ordering between two unsynchronised workers.
	goodDeadline := time.After(10 * time.Second)
	for goodRebuilds.Load() == 0 {
		select {
		case <-goodDeadline:
			t.Fatal("good key never rebuilt while the bad key was failing")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if n := goodRebuilds.Load(); n != 1 {
		t.Errorf("good key rebuilt %d times, want exactly 1", n)
	}
	mu.Lock()
	var sawRebuildErr bool
	for _, e := range errs {
		if strings.Contains(e, `rebuild "bad"`) {
			sawRebuildErr = true
		}
	}
	errsSnapshot := append([]string(nil), errs...) // copy: safe to read after mu.Unlock()
	mu.Unlock()
	if !sawRebuildErr {
		t.Errorf("OnError missing rebuild-failure report: %v", errsSnapshot)
	}
	cancel()
	<-done
}

func TestRunCalledTwiceReturnsError(t *testing.T) {
	src := &fakeSource{}
	r, err := New(
		Source(src), Scope(keyScope),
		Rebuild(func(context.Context, Key) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // first Run can be cancelled immediately
	if err := r.Run(ctx); err != context.Canceled {
		t.Fatalf("first Run() = %v, want context.Canceled", err)
	}
	err = r.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "already") {
		t.Fatalf("second Run() = %v, want error mentioning \"already\"", err)
	}
}

func TestWorkerLoopSilentOnCancelledRebuild(t *testing.T) {
	rebuildStarted := make(chan struct{})
	var onErrorCalls atomic.Int64
	src := &fakeSource{queue: []Event{{Row: rowFor("stuck")}}}
	r, err := New(
		Source(src), Scope(keyScope),
		Rebuild(func(ctx context.Context, k Key) error {
			close(rebuildStarted)
			<-ctx.Done()
			return ctx.Err()
		}),
		Coalesce(time.Millisecond), MaxWait(10*time.Millisecond),
		OnError(func(error) { onErrorCalls.Add(1) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case <-rebuildStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild never started")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if n := onErrorCalls.Load(); n != 0 {
		t.Errorf("OnError called %d times, want 0", n)
	}
	s := r.Stats()
	if s.RebuildsFailed != 0 {
		t.Errorf("RebuildsFailed = %d, want 0", s.RebuildsFailed)
	}
	if s.PoisonedKeys != 0 {
		t.Errorf("PoisonedKeys = %d, want 0", s.PoisonedKeys)
	}
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

func TestReceiveLoopSkipsErrSkipWrappedErrors(t *testing.T) {
	var rebuilds atomic.Int64
	var mu sync.Mutex
	var onErrs []error
	src := &seqSource{seq: []func() (Event, error){
		func() (Event, error) { return Event{Row: rowFor("a")}, nil },
		func() (Event, error) { return Event{}, fmt.Errorf("%w: undecodable message", ErrSkip) },
		func() (Event, error) { return Event{Row: rowFor("b")}, nil },
	}}
	r, err := New(
		Source(src), Scope(keyScope),
		Rebuild(func(_ context.Context, k Key) error { rebuilds.Add(1); return nil }),
		Coalesce(time.Millisecond), MaxWait(10*time.Millisecond),
		OnError(func(e error) { mu.Lock(); onErrs = append(onErrs, e); mu.Unlock() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for rebuilds.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("want 2 rebuilds (a and b), got %d", rebuilds.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	if s := r.Stats().EventsSkipped; s != 1 {
		t.Errorf("EventsSkipped = %d, want 1", s)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(onErrs) != 1 {
		t.Fatalf("OnError called %d times, want 1: %v", len(onErrs), onErrs)
	}
	if !errors.Is(onErrs[0], ErrSkip) {
		t.Errorf("OnError arg %v does not satisfy errors.Is(err, ErrSkip)", onErrs[0])
	}
}

func TestStatsTimestampsAfterActivity(t *testing.T) {
	start := time.Now()
	src := &fakeSource{queue: []Event{{Row: rowFor("a")}}}
	r, err := New(
		Source(src), Scope(keyScope),
		Rebuild(func(context.Context, Key) error { return nil }),
		Coalesce(time.Millisecond), MaxWait(10*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for r.Stats().RebuildsOK == 0 {
		select {
		case <-deadline:
			t.Fatal("no rebuild within 2s")
		case <-time.After(5 * time.Millisecond):
		}
	}
	s := r.Stats()
	cancel()
	<-done

	if s.LastEvent.Before(start) {
		t.Errorf("LastEvent = %v, want at/after test start %v", s.LastEvent, start)
	}
	if s.LastRebuild.Before(start) {
		t.Errorf("LastRebuild = %v, want at/after test start %v", s.LastRebuild, start)
	}
}

func TestReceiveLoopFatalErrorStillKillsRun(t *testing.T) {
	fatal := errors.New("connection lost")
	src := &seqSource{seq: []func() (Event, error){
		func() (Event, error) { return Event{}, fatal },
	}}
	r, err := New(
		Source(src), Scope(keyScope),
		Rebuild(func(context.Context, Key) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	runErr := r.Run(context.Background())
	if runErr == nil || !errors.Is(runErr, fatal) {
		t.Fatalf("Run() = %v, want an error wrapping %v", runErr, fatal)
	}
}
