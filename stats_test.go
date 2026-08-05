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
