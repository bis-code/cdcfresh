package cdcfresh

import "time"

// retryDelay returns the wait before retry number attempt (1-based):
// base·2^(attempt-1) capped at max, then jittered into [d/2, d).
// jitter must return a value in [0, 1).
func retryDelay(base, max time.Duration, attempt int, jitter func() float64) time.Duration {
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	half := d / 2
	return half + time.Duration(jitter()*float64(half))
}
