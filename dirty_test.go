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
