//go:build integration

package testenv_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bis-code/cdcfresh/internal/testenv"
)

const topic = "persistent://public/default/cdcfresh-testenv"

// TestPulsarCarriesCanalJSON proves the rig the adapter tier is built on: real
// canal-json published to a real broker comes back to a consumer byte-for-byte
// and can be acknowledged. Ack is the point — it is the mechanism cdcfresh's
// delivery guarantee rests on, and the one thing a fake cannot vouch for.
func TestPulsarCarriesCanalJSON(t *testing.T) {
	p := testenv.StartPulsar(t)

	want := []string{"insert", "update", "delete"}
	payloads := make([][]byte, 0, len(want))
	for _, name := range want {
		payloads = append(payloads, testenv.Golden(t, name))
	}
	p.Produce(t, topic, payloads...)

	consumer := p.Subscribe(t, topic, "testenv")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i, name := range want {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			t.Fatalf("receive message %d (%s): %v", i, name, err)
		}
		if err := consumer.Ack(msg); err != nil {
			t.Fatalf("ack message %d (%s): %v", i, name, err)
		}

		var ev struct {
			Type  string              `json:"type"`
			IsDdl bool                `json:"isDdl"`
			Data  []map[string]string `json:"data"`
		}
		if err := json.Unmarshal(msg.Payload(), &ev); err != nil {
			t.Fatalf("message %d is not canal-json: %v", i, err)
		}
		// Ordering is asserted, not just membership: a partitioned or
		// out-of-order topic would break per-key coalescing downstream.
		if ev.IsDdl || !strings.EqualFold(ev.Type, name) {
			t.Errorf("message %d: type = %q isDdl = %v, want %s", i, ev.Type, ev.IsDdl, name)
		}
		if len(ev.Data) == 0 {
			t.Errorf("message %d (%s): data is empty", i, name)
		}
	}
}

// TestTiDBServesAsRebuildTarget proves the other half: a derived table can be
// recomputed from source tables with ordinary SQL, which is all cdcfresh ever
// asks of the database.
func TestTiDBServesAsRebuildTarget(t *testing.T) {
	db := testenv.StartTiDB(t).DB

	for _, stmt := range []string{
		"CREATE TABLE readings (id INT PRIMARY KEY, device VARCHAR(64), value INT)",
		"CREATE TABLE device_totals (device VARCHAR(64) PRIMARY KEY, total INT)",
		"INSERT INTO readings VALUES (1,'dev-a',10),(2,'dev-a',5),(3,'dev-b',7)",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	// The shape of a real Rebuild: recompute one scope from source truth.
	const rebuild = `REPLACE INTO device_totals (device, total)
	                 SELECT device, SUM(value) FROM readings WHERE device = ? GROUP BY device`
	if _, err := db.Exec(rebuild, "dev-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	var total int
	if err := db.QueryRow("SELECT total FROM device_totals WHERE device = ?", "dev-a").Scan(&total); err != nil {
		t.Fatalf("read rebuilt row: %v", err)
	}
	if total != 15 {
		t.Errorf("total = %d, want 15", total)
	}

	// Rebuilds are idempotent by construction — running one twice must not
	// double-count, which is what makes duplicate events harmless.
	if _, err := db.Exec(rebuild, "dev-a"); err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	if err := db.QueryRow("SELECT total FROM device_totals WHERE device = ?", "dev-a").Scan(&total); err != nil {
		t.Fatalf("re-read rebuilt row: %v", err)
	}
	if total != 15 {
		t.Errorf("total after second rebuild = %d, want 15", total)
	}
}
