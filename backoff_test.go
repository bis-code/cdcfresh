package cdcfresh

import (
	"testing"
	"time"
)

func TestRetryDelayDoublesAndCaps(t *testing.T) {
	noJitter := func() float64 { return 0 } // lower bound: exactly d/2
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 500 * time.Millisecond}, // 1s/2
		{2, time.Second},            // 2s/2
		{3, 2 * time.Second},        // 4s/2
		{10, 150 * time.Second},     // capped at 5min → /2
		{100, 150 * time.Second},    // stays capped, no overflow
	}
	for _, c := range cases {
		got := retryDelay(time.Second, 5*time.Minute, c.attempt, noJitter)
		if got != c.want {
			t.Errorf("attempt %d: got %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestRetryDelayJitterBounds(t *testing.T) {
	almostOne := func() float64 { return 0.999 }
	d := retryDelay(time.Second, 5*time.Minute, 2, almostOne) // base delay 2s
	if d < time.Second || d >= 2*time.Second {
		t.Errorf("jittered delay %v outside [1s, 2s)", d)
	}
}
