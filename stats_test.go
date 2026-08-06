package cdcfresh

import (
	"testing"
	"time"
)

func TestStatsSnapshot(t *testing.T) {
	r, err := New(requiredOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	r.stats.eventsReceived.Add(3)
	r.stats.rebuildsOK.Add(2)
	r.dirty.mark("a", time.Now())
	s := r.Stats()
	if s.EventsReceived != 3 || s.RebuildsOK != 2 || s.DirtyKeys != 1 || s.PoisonedKeys != 0 {
		t.Errorf("snapshot wrong: %+v", s)
	}
}

func TestStatsTimestampsZeroBeforeActivity(t *testing.T) {
	r, err := New(requiredOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	s := r.Stats()
	if !s.LastEvent.IsZero() {
		t.Errorf("LastEvent = %v, want zero before any event", s.LastEvent)
	}
	if !s.LastRebuild.IsZero() {
		t.Errorf("LastRebuild = %v, want zero before any rebuild", s.LastRebuild)
	}
	if s.EventsSkipped != 0 {
		t.Errorf("EventsSkipped = %d, want 0", s.EventsSkipped)
	}
}
