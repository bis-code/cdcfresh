//go:build integration

// Package harness holds the integration smoke test for the local TiDB + TiCDC
// + Pulsar stack. It proves the rig itself works — a row written to TiDB comes
// out of Pulsar as canal-json — which is the foundation every later
// integration test builds on.
//
// It reads Pulsar through the same client the adapter will use, subscribing
// before the write and acknowledging what it receives. The admin REST API
// could report a message count more cheaply, but counting messages an operator
// endpoint can see is not evidence that a consumer can subscribe, decode and
// ack one, and acking is the exact mechanism cdcfresh's delivery guarantee
// rests on.
package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	_ "github.com/go-sql-driver/mysql"
)

const (
	tidbDSN    = "root@tcp(127.0.0.1:4000)/"
	tidbStatus = "http://127.0.0.1:10080/status"
	pulsarURL  = "pulsar://127.0.0.1:6650"
	topic      = "persistent://public/default/cdcfresh"

	// TiCDC batches before it flushes, so a row is not on the topic instantly.
	deliveryTimeout = 60 * time.Second
)

const hint = "\nrun `make harness-up` first"

// requireTiDB opens TiDB, failing — rather than skipping — when it is
// unreachable. A skip here would make an unrun integration tier look identical
// to a passing one, which is the failure mode this tier exists to prevent.
func requireTiDB(t *testing.T) *sql.DB {
	t.Helper()
	resp, err := http.Get(tidbStatus)
	if err != nil {
		t.Fatalf("TiDB unreachable at %s (%v)%s", tidbStatus, err, hint)
	}
	resp.Body.Close()

	db, err := sql.Open("mysql", tidbDSN)
	if err != nil {
		t.Fatalf("open tidb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping tidb: %v%s", err, hint)
	}
	return db
}

// subscribe returns a consumer positioned at the end of the topic, so it sees
// only what is produced after this call and leftovers from earlier runs cannot
// satisfy the assertion. The subscription name is unique per run: a shared one
// would carry its cursor across runs and turn every re-run into a replay.
func subscribe(t *testing.T) pulsar.Consumer {
	t.Helper()
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               pulsarURL,
		ConnectionTimeout: 10 * time.Second,
		OperationTimeout:  30 * time.Second,
		Logger:            plog.DefaultNopLogger(),
	})
	if err != nil {
		t.Fatalf("pulsar client at %s: %v%s", pulsarURL, err, hint)
	}
	t.Cleanup(client.Close)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            fmt.Sprintf("harness-%d", time.Now().UnixNano()),
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})
	if err != nil {
		t.Fatalf("subscribe to %s: %v%s", topic, err, hint)
	}
	t.Cleanup(func() {
		consumer.Unsubscribe()
		consumer.Close()
	})
	return consumer
}

// TestHarnessDeliversCanalJSON writes a row to TiDB and asserts a Pulsar
// consumer receives it as canal-json naming that table.
func TestHarnessDeliversCanalJSON(t *testing.T) {
	db := requireTiDB(t)
	consumer := subscribe(t)

	// A per-run table keeps repeat runs independent.
	table := fmt.Sprintf("smoke_%d", time.Now().UnixNano())
	for _, stmt := range []string{
		"CREATE DATABASE IF NOT EXISTS demo",
		fmt.Sprintf("CREATE TABLE demo.%s (id INT PRIMARY KEY, device VARCHAR(64), value INT)", table),
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() { db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS demo.%s", table)) })

	if _, err := db.Exec(fmt.Sprintf("INSERT INTO demo.%s VALUES (1, 'dev-a', 10)", table)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()

	// The changefeed covers the whole cluster, so this topic also carries DDL
	// and other tables' rows. Match on the decoded fields rather than a
	// substring: the CREATE TABLE statement above names the table too, and a
	// substring check happily accepts that DDL as proof the row arrived.
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			t.Fatalf("no INSERT on demo.%s within %s of the insert: %v", table, deliveryTimeout, err)
		}
		payload := msg.Payload()
		if err := consumer.Ack(msg); err != nil {
			t.Fatalf("ack: %v", err)
		}

		var ev canalEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			t.Fatalf("message is not canal-json: %v\n%s", err, payload)
		}
		if ev.IsDdl || ev.Table != table || !strings.EqualFold(ev.Type, "insert") {
			continue
		}
		if len(ev.Data) != 1 {
			t.Fatalf("want 1 row in data, got %d:\n%s", len(ev.Data), payload)
		}
		if got := ev.Data[0]["device"]; got != "dev-a" {
			t.Errorf("device = %q, want %q:\n%s", got, "dev-a", payload)
		}
		t.Logf("received and acked: %s", payload)
		return
	}
}

// canalEvent is the subset of the canal-json envelope these tests assert on.
// The decoder in issue #3 owns the full shape; this stays deliberately small.
type canalEvent struct {
	Database string              `json:"database"`
	Table    string              `json:"table"`
	Type     string              `json:"type"`
	IsDdl    bool                `json:"isDdl"`
	PKNames  []string            `json:"pkNames"`
	Data     []map[string]string `json:"data"`
}

// TestGoldenFilesAreRealCanalJSON guards the captured fixtures: they must stay
// parseable and keep the shape the decoder in issue #3 will rely on.
func TestGoldenFilesAreRealCanalJSON(t *testing.T) {
	for _, name := range []string{"insert", "update", "delete"} {
		t.Run(name, func(t *testing.T) {
			raw := readGolden(t, name)
			var ev canalEvent
			if err := json.Unmarshal(raw, &ev); err != nil {
				t.Fatalf("golden %s.json is not valid canal-json: %v", name, err)
			}
			if ev.IsDdl {
				t.Errorf("expected a row event, got isDdl=true")
			}
			if ev.Table == "" || ev.Database == "" {
				t.Errorf("missing database/table: %+v", ev)
			}
			if len(ev.Data) == 0 {
				t.Errorf("data is empty — canal-json carries rows as an array, decoder must fan out")
			}
			if !strings.EqualFold(ev.Type, name) {
				t.Errorf("type = %q, want %q", ev.Type, name)
			}
		})
	}
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "internal", "canaljson", "testdata", name+".json"))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return b
}
