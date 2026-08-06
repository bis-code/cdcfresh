package cdcfresh

import "testing"

func TestEventTypeString(t *testing.T) {
	cases := []struct {
		et   EventType
		want string
	}{
		{Unknown, "unknown"},
		{Insert, "insert"},
		{Update, "update"},
		{Delete, "delete"},
		{EventType(99), "EventType(99)"},
	}
	for _, tc := range cases {
		if got := tc.et.String(); got != tc.want {
			t.Errorf("EventType(%d).String() = %q, want %q", uint8(tc.et), got, tc.want)
		}
	}
}
