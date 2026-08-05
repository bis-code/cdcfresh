package cdcfresh

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)

// stub backoff: attempt n → n seconds, deterministic.
func newTestSet() *dirtySet {
	return newDirtySet(5*time.Second, 30*time.Second, 3,
		func(n int) time.Duration { return time.Duration(n) * time.Second })
}

func TestPopEmpty(t *testing.T) {
	d := newTestSet()
	if k, ok := d.pop(t0); ok {
		t.Fatalf("pop on empty set returned %q", k)
	}
}

func TestPopWaitsForQuietPeriod(t *testing.T) {
	d := newTestSet()
	d.mark("a", t0)
	if k, ok := d.pop(t0.Add(4999 * time.Millisecond)); ok {
		t.Fatalf("popped %q before 5s quiet period", k)
	}
	k, ok := d.pop(t0.Add(5 * time.Second))
	if !ok || k != "a" {
		t.Fatalf("want a after quiet period, got %q %v", k, ok)
	}
}

func TestNewEventExtendsQuietPeriod(t *testing.T) {
	d := newTestSet()
	d.mark("a", t0)
	d.mark("a", t0.Add(3*time.Second))               // debounce: window restarts
	if k, ok := d.pop(t0.Add(7 * time.Second)); ok { // 4s after last event
		t.Fatalf("popped %q 4s after last event", k)
	}
	if _, ok := d.pop(t0.Add(8 * time.Second)); !ok {
		t.Fatal("want ready 5s after last event")
	}
}

func TestHotKeyFiresAtMaxWait(t *testing.T) {
	d := newTestSet()
	for i := 0; i <= 29; i++ { // an event every second: never quiet
		d.mark("a", t0.Add(time.Duration(i)*time.Second))
	}
	if k, ok := d.pop(t0.Add(29 * time.Second)); ok {
		t.Fatalf("popped %q before maxWait", k)
	}
	k, ok := d.pop(t0.Add(30 * time.Second)) // firstDirty + 30s
	if !ok || k != "a" {
		t.Fatalf("want a at maxWait, got %q %v", k, ok)
	}
}

func TestPopFIFOAcrossKeys(t *testing.T) {
	d := newTestSet()
	d.mark("a", t0)
	d.mark("b", t0.Add(time.Second))
	at := t0.Add(10 * time.Second)
	k1, _ := d.pop(at)
	k2, _ := d.pop(at)
	if k1 != "a" || k2 != "b" {
		t.Fatalf("want FIFO a,b — got %q,%q", k1, k2)
	}
}

func TestSingleFlight(t *testing.T) {
	d := newTestSet()
	d.mark("a", t0)
	at := t0.Add(10 * time.Second)
	if _, ok := d.pop(at); !ok {
		t.Fatal("first pop should return a")
	}
	if k, ok := d.pop(at); ok {
		t.Fatalf("popped %q while a is in flight", k)
	}
}

func TestCompleteSuccessRemoves(t *testing.T) {
	d := newTestSet()
	d.mark("a", t0)
	at := t0.Add(10 * time.Second)
	d.pop(at)
	if poisoned := d.complete("a", nil, at.Add(time.Second)); poisoned {
		t.Fatal("success must not poison")
	}
	if _, ok := d.pop(at.Add(time.Hour)); ok {
		t.Fatal("completed key must be gone")
	}
	if len(d.entries) != 0 {
		t.Fatalf("entries leak: %d", len(d.entries))
	}
}

func TestRedirtyDuringRebuildNotLost(t *testing.T) {
	d := newTestSet()
	d.mark("a", t0)
	at := t0.Add(10 * time.Second)
	d.pop(at)
	d.mark("a", at.Add(time.Second)) // event lands mid-rebuild
	done := at.Add(2 * time.Second)
	d.complete("a", nil, done)
	if k, ok := d.pop(done.Add(4 * time.Second)); ok { // 4s < fresh 5s quiet
		t.Fatalf("redirtied key %q ready before new quiet period", k)
	}
	k, ok := d.pop(done.Add(5 * time.Second))
	if !ok || k != "a" {
		t.Fatalf("redirtied key must re-enter pipeline, got %q %v", k, ok)
	}
}

func TestRedirtyReentersAtTail(t *testing.T) {
	d := newTestSet()
	d.mark("a", t0)
	at := t0.Add(10 * time.Second)
	d.pop(at)
	d.mark("a", at)          // redirty a while in flight
	d.mark("b", at)          // b arrives while a rebuilds
	d.complete("a", nil, at) // a re-enters at tail, behind b
	late := at.Add(10 * time.Second)
	k1, _ := d.pop(late)
	k2, _ := d.pop(late)
	if k1 != "b" || k2 != "a" {
		t.Fatalf("want b then a (tail re-entry), got %q,%q", k1, k2)
	}
}
