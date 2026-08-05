package cdcfresh

import (
	"errors"
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

func popAndFail(d *dirtySet, k Key, at time.Time) bool {
	got, ok := d.pop(at)
	if !ok || got != k {
		panic("test setup: expected " + string(k) + " ready")
	}
	return d.complete(k, errStub, at)
}

var errStub = errors.New("rebuild failed")

func TestFailureSchedulesRetryWithBackoff(t *testing.T) {
	d := newTestSet() // stub backoff: attempt n → n seconds
	d.mark("a", t0)
	at := t0.Add(10 * time.Second)
	if poisoned := popAndFail(d, "a", at); poisoned {
		t.Fatal("first failure must not poison")
	}
	if k, ok := d.pop(at.Add(999 * time.Millisecond)); ok {
		t.Fatalf("popped %q before retryAt", k)
	}
	if _, ok := d.pop(at.Add(time.Second)); !ok {
		t.Fatal("want ready at retryAt (attempt 1 → +1s)")
	}
}

func TestPoisonAfterConsecutiveFailures(t *testing.T) {
	d := newTestSet() // poisonAfter = 3
	d.mark("a", t0)
	at := t0.Add(10 * time.Second)
	if popAndFail(d, "a", at) { // fail 1
		t.Fatal("poisoned too early")
	}
	at = at.Add(time.Second)
	if popAndFail(d, "a", at) { // fail 2
		t.Fatal("poisoned too early")
	}
	at = at.Add(2 * time.Second)
	if !popAndFail(d, "a", at) { // fail 3 → poison, reported once
		t.Fatal("want poisoned=true on 3rd failure")
	}
	if k, ok := d.pop(at.Add(time.Hour)); ok {
		t.Fatalf("poisoned key %q popped", k)
	}
	d.mark("a", at.Add(time.Minute)) // CDC events ignored while poisoned
	if k, ok := d.pop(at.Add(2 * time.Hour)); ok {
		t.Fatalf("mark revived poisoned key %q", k)
	}
	if dirty, poisoned := d.counts(); poisoned != 1 || dirty != 0 {
		t.Fatalf("counts: dirty=%d poisoned=%d", dirty, poisoned)
	}
}

func TestReconcileRevivesPoisonedKey(t *testing.T) {
	d := newTestSet()
	d.mark("a", t0)
	at := t0.Add(10 * time.Second)
	popAndFail(d, "a", at)
	at = at.Add(time.Second)
	popAndFail(d, "a", at)
	at = at.Add(2 * time.Second)
	popAndFail(d, "a", at) // poisoned
	d.markReconcile("a", at.Add(time.Minute))
	k, ok := d.pop(at.Add(time.Minute)) // immediately ready (retryAt = now)
	if !ok || k != "a" {
		t.Fatalf("reconcile must revive poisoned key, got %q %v", k, ok)
	}
	if d.complete("a", nil, at.Add(2*time.Minute)) {
		t.Fatal("success after revive must not poison")
	}
	if _, poisoned := d.counts(); poisoned != 0 {
		t.Fatal("success must clear poison accounting")
	}
}

func TestNextWake(t *testing.T) {
	d := newTestSet()
	if _, ok := d.nextWake(t0); ok {
		t.Fatal("empty set has no wake time")
	}
	d.mark("a", t0) // quiet deadline t0+5s, maxWait t0+30s
	w, ok := d.nextWake(t0)
	if !ok || !w.Equal(t0.Add(5*time.Second)) {
		t.Fatalf("want wake t0+5s, got %v %v", w, ok)
	}
	d.mark("a", t0.Add(28*time.Second)) // quiet t0+33s vs maxWait t0+30s
	w, _ = d.nextWake(t0.Add(28 * time.Second))
	if !w.Equal(t0.Add(30 * time.Second)) {
		t.Fatalf("want maxWait deadline t0+30s, got %v", w)
	}
}
