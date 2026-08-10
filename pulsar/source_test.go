package pulsar

import (
	"strings"
	"testing"

	"github.com/bis-code/cdcfresh"
)

// TestSourceReportsEveryProblemAtOnce mirrors cdcfresh.New: one error listing
// everything wrong, so a caller fixing arguments does not discover them one
// failed run at a time.
func TestSourceReportsEveryProblemAtOnce(t *testing.T) {
	src, err := Source("", nil)
	if err == nil {
		src.Close()
		t.Fatal("Source succeeded with no url and no topics, want an error")
	}
	for _, want := range []string{"url", "topics"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestSourceRejectsBadArguments(t *testing.T) {
	for _, tt := range []struct {
		name    string
		url     string
		topics  []string
		opts    []Option
		wantErr string
	}{
		{"blank url", "   ", []string{"t"}, nil, "url"},
		{"no topics", "pulsar://localhost:6650", nil, nil, "topics"},
		{"empty topic entry", "pulsar://localhost:6650", []string{"t", ""}, nil, "topics"},
		{
			"empty subscription", "pulsar://localhost:6650", []string{"t"},
			[]Option{WithSubscription("  ")}, "subscription",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			src, err := Source(tt.url, tt.topics, tt.opts...)
			if err == nil {
				src.Close()
				t.Fatal("Source succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

// TestBatchAcksOnlyAfterLastRow is the ack contract in miniature. A message
// carrying several rows must not be acknowledged until every one of them has
// been handed over, or a crash mid-batch loses the rows still queued —
// exactly the durability the broker cursor is supposed to provide.
func TestBatchAcksOnlyAfterLastRow(t *testing.T) {
	var acked int
	b := batch{
		rows: []cdcfresh.RowEvent{
			{Table: "a", Type: cdcfresh.Insert},
			{Table: "b", Type: cdcfresh.Insert},
			{Table: "c", Type: cdcfresh.Insert},
		},
		ack: func() { acked++ },
	}

	for i, wantTable := range []string{"a", "b", "c"} {
		ev, ok := b.next()
		if !ok {
			t.Fatalf("row %d: next() reported empty, want an event", i)
		}
		if ev.Row.Table != wantTable {
			t.Errorf("row %d: table = %q, want %q", i, ev.Row.Table, wantTable)
		}

		last := i == 2
		if got := ev.Ack != nil; got != last {
			t.Errorf("row %d: has ack = %v, want %v", i, got, last)
		}
		if ev.Ack != nil {
			ev.Ack()
		}
		if want := boolToInt(last); acked != want {
			t.Errorf("after row %d: acked %d times, want %d", i, acked, want)
		}
	}

	if _, ok := b.next(); ok {
		t.Error("next() returned a fourth event, want the batch drained")
	}
	if acked != 1 {
		t.Errorf("acked %d times, want exactly 1", acked)
	}
}

// TestBatchSingleRowAcksImmediately: the common case is one row per message,
// where the only row is also the last one.
func TestBatchSingleRowAcksImmediately(t *testing.T) {
	var acked bool
	b := batch{
		rows: []cdcfresh.RowEvent{{Table: "only"}},
		ack:  func() { acked = true },
	}

	ev, ok := b.next()
	if !ok {
		t.Fatal("next() reported empty")
	}
	if ev.Ack == nil {
		t.Fatal("single-row event carries no ack; its message would never be retired")
	}
	ev.Ack()
	if !acked {
		t.Error("ack did not reach the message")
	}
}

func TestBatchEmptyReportsDrained(t *testing.T) {
	var b batch
	if _, ok := b.next(); ok {
		t.Error("next() on a zero batch returned an event")
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
