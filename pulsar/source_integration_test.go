//go:build integration

package pulsar_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bis-code/cdcfresh"
	"github.com/bis-code/cdcfresh/internal/testenv"
	"github.com/bis-code/cdcfresh/pulsar"
)

func topicName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("persistent://public/default/%s-%d", t.Name(), time.Now().UnixNano())
}

// TestSourceDeliversRowEventsAndAcks runs the adapter against a real broker
// carrying the exact bytes a TiCDC changefeed produces: row events, DDL, and
// something undecodable. It also proves the acks stick, by reconnecting on
// the same subscription and requiring silence — an ack that never reached the
// broker looks identical to a working one until a restart replays everything.
func TestSourceDeliversRowEventsAndAcks(t *testing.T) {
	p := testenv.StartPulsar(t)
	topic := topicName(t)
	const subscription = "adapter-acks"

	p.Produce(t, topic,
		testenv.Golden(t, "insert"),
		testenv.Golden(t, "ddl_create"), // dropped silently, still acked
		testenv.Golden(t, "update"),
		testenv.Golden(t, "delete"),
		[]byte(`{"database":`), // undecodable: ErrSkip, still acked
	)

	src, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription(subscription))
	if err != nil {
		t.Fatalf("Source: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The DDL message sits between the insert and the update; receiving three
	// row events in this order shows it was dropped rather than surfaced.
	for _, want := range []cdcfresh.EventType{cdcfresh.Insert, cdcfresh.Update, cdcfresh.Delete} {
		ev, err := src.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive %v: %v", want, err)
		}
		if ev.Row.Type != want {
			t.Fatalf("got %v, want %v", ev.Row.Type, want)
		}
		if ev.Row.Table != "readings" {
			t.Errorf("table = %q, want readings", ev.Row.Table)
		}
		if ev.Ack == nil {
			t.Fatalf("%v event carries no ack; its message would never be retired", want)
		}
		ev.Ack()
	}

	// The undecodable payload must be a per-message skip, not a source
	// failure: one bad message cannot be allowed to stop the pipeline.
	if _, err := src.Receive(ctx); !errors.Is(err, cdcfresh.ErrSkip) {
		t.Fatalf("undecodable message gave err = %v, want one wrapping ErrSkip", err)
	}

	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Errorf("second Close: %v, want it to be safe to repeat", err)
	}

	// Reconnecting on the same subscription: anything unacked comes back.
	src2, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription(subscription))
	if err != nil {
		t.Fatalf("re-subscribe: %v", err)
	}
	defer src2.Close()

	quiet, cancelQuiet := context.WithTimeout(ctx, 5*time.Second)
	defer cancelQuiet()
	ev, err := src2.Receive(quiet)
	if err == nil {
		t.Fatalf("a %v event on %s.%s was redelivered, so it was never acked",
			ev.Row.Type, ev.Row.Database, ev.Row.Table)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("re-subscribe failed with %v, want a quiet timeout", err)
	}
}

// TestRefresherRebuildsDerivedTable is the whole premise end to end: a
// canal-json message names a scope, cdcfresh coalesces it, and the rebuild
// recomputes that scope from source tables in a real database. Nothing from
// the event body reaches the SQL — the doorbell only says which device to
// recompute.
func TestRefresherRebuildsDerivedTable(t *testing.T) {
	p := testenv.StartPulsar(t)
	db := testenv.StartTiDB(t).DB
	topic := topicName(t)

	for _, stmt := range []string{
		"CREATE TABLE readings (id INT PRIMARY KEY, device VARCHAR(64), value INT)",
		"CREATE TABLE device_totals (device VARCHAR(64) PRIMARY KEY, total INT)",
		// Source truth the rebuild will read. The event body claims value 10
		// for a single row; the table says otherwise, and the table wins.
		"INSERT INTO readings VALUES (1,'dev-a',10),(2,'dev-a',32),(3,'dev-b',7)",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	p.Produce(t, topic, testenv.Golden(t, "insert"))

	src, err := pulsar.Source(p.URL, []string{topic}, pulsar.WithSubscription("refresher"))
	if err != nil {
		t.Fatalf("Source: %v", err)
	}
	defer src.Close()

	rebuilt := make(chan cdcfresh.Key, 4)
	r, err := cdcfresh.New(
		cdcfresh.Source(src),
		cdcfresh.Scope(func(ev cdcfresh.RowEvent) []cdcfresh.Key {
			row := ev.Data
			if row == nil {
				row = ev.Old // deletes carry the previous image
			}
			device, _ := row["device"].(string)
			if device == "" {
				return nil
			}
			return []cdcfresh.Key{cdcfresh.Key(device)}
		}),
		cdcfresh.Rebuild(func(ctx context.Context, k cdcfresh.Key) error {
			_, err := db.ExecContext(ctx,
				`REPLACE INTO device_totals (device, total)
				 SELECT device, SUM(value) FROM readings WHERE device = ? GROUP BY device`,
				string(k))
			if err == nil {
				rebuilt <- k
			}
			return err
		}),
		cdcfresh.Coalesce(200*time.Millisecond),
		cdcfresh.MaxWait(2*time.Second),
		cdcfresh.OnError(func(err error) { t.Logf("cdcfresh: %v", err) }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	select {
	case k := <-rebuilt:
		if k != "dev-a" {
			t.Fatalf("rebuilt scope %q, want dev-a", k)
		}
	case err := <-done:
		t.Fatalf("Run returned before any rebuild: %v", err)
	case <-time.After(45 * time.Second):
		t.Fatal("no rebuild within 45s of the event landing on the topic")
	}

	var total int
	if err := db.QueryRow("SELECT total FROM device_totals WHERE device = ?", "dev-a").Scan(&total); err != nil {
		t.Fatalf("read derived table: %v", err)
	}
	// 42 is the sum in the source table, not the 10 the event carried:
	// evidence the rebuild recomputed rather than applying the payload.
	if total != 42 {
		t.Errorf("device_totals[dev-a] = %d, want 42 (the sum in readings, not the event's value)", total)
	}

	// Only the scope the doorbell named was rebuilt.
	var others int
	if err := db.QueryRow("SELECT COUNT(*) FROM device_totals WHERE device <> ?", "dev-a").Scan(&others); err != nil {
		t.Fatalf("count other scopes: %v", err)
	}
	if others != 0 {
		t.Errorf("%d unrelated scopes were rebuilt, want 0", others)
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Run: %v", err)
	}

	if got := r.Stats().EventsReceived; got != 1 {
		t.Errorf("EventsReceived = %d, want 1", got)
	}
}
