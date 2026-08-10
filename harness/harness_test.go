//go:build integration

// Package harness holds the integration smoke test for the local TiDB + TiCDC
// + Pulsar stack. It proves the rig itself works — a row written to TiDB comes
// out of Pulsar as canal-json — which is the foundation every later
// integration test builds on.
//
// It deliberately talks to Pulsar over the admin REST API rather than a Pulsar
// client library: the client dependency belongs to the adapter package, not to
// the harness.
package harness

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	tidbDSN     = "root@tcp(127.0.0.1:4000)/"
	tidbStatus  = "http://127.0.0.1:10080/status"
	pulsarAdmin = "http://127.0.0.1:8080"
	topic       = "persistent://public/default/cdcfresh"
	topicPath   = "public/default/cdcfresh"
)

// requireHarness fails — rather than skips — when the stack is unreachable. A
// skip here would make an unrun integration tier look identical to a passing
// one, which is the failure mode this tier exists to prevent.
func requireHarness(t *testing.T) {
	t.Helper()
	for _, probe := range []struct{ name, url string }{
		{"TiDB", tidbStatus},
		{"Pulsar", pulsarAdmin + "/admin/v2/brokers/health"},
	} {
		resp, err := http.Get(probe.url)
		if err != nil {
			t.Fatalf("%s unreachable at %s (%v)\nrun `make harness-up` first", probe.name, probe.url, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s unhealthy at %s (status %d)\nrun `make harness-up` first", probe.name, probe.url, resp.StatusCode)
		}
	}
}

func msgCount(t *testing.T) float64 {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/admin/v2/persistent/%s/stats", pulsarAdmin, topicPath))
	if err != nil {
		t.Fatalf("topic stats: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("topic stats: status %d (is the changefeed created? run `make harness-up`)", resp.StatusCode)
	}
	var stats struct {
		MsgInCounter float64 `json:"msgInCounter"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode topic stats: %v", err)
	}
	return stats.MsgInCounter
}

// newestMessage returns the body of the most recent message on the topic.
func newestMessage(t *testing.T) string {
	t.Helper()
	u := fmt.Sprintf("%s/admin/v2/persistent/%s/examinemessage?initialPosition=latest&messagePosition=1",
		pulsarAdmin, topicPath)
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("examine message: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read message body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("examine message: status %d: %s", resp.StatusCode, body)
	}
	return string(body)
}

// TestHarnessDeliversCanalJSON writes a row to TiDB and asserts it comes out of
// Pulsar as canal-json naming that table.
func TestHarnessDeliversCanalJSON(t *testing.T) {
	requireHarness(t)

	db, err := sql.Open("mysql", tidbDSN)
	if err != nil {
		t.Fatalf("open tidb: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("ping tidb: %v\nrun `make harness-up` first", err)
	}

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

	before := msgCount(t)
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO demo.%s VALUES (1, 'dev-a', 10)", table)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// TiCDC batches before it flushes, so the message is not instant.
	deadline := time.Now().Add(60 * time.Second)
	for msgCount(t) <= before {
		if time.Now().After(deadline) {
			t.Fatalf("no new message on %s within 60s of the insert (msgInCounter stuck at %v)", topic, before)
		}
		time.Sleep(500 * time.Millisecond)
	}

	msg := newestMessage(t)
	if !strings.Contains(msg, table) {
		t.Errorf("newest message does not mention table %q:\n%s", table, msg)
	}
	if !strings.Contains(msg, `"isDdl"`) {
		t.Errorf("newest message is not canal-json (no isDdl field):\n%s", msg)
	}
}

// TestGoldenFilesAreRealCanalJSON guards the captured fixtures: they must stay
// parseable and keep the shape the decoder in issue #3 will rely on.
func TestGoldenFilesAreRealCanalJSON(t *testing.T) {
	for _, name := range []string{"insert", "update", "delete"} {
		t.Run(name, func(t *testing.T) {
			raw := readGolden(t, name)
			var ev struct {
				Database string              `json:"database"`
				Table    string              `json:"table"`
				Type     string              `json:"type"`
				IsDdl    bool                `json:"isDdl"`
				PKNames  []string            `json:"pkNames"`
				Data     []map[string]string `json:"data"`
			}
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
			if strings.ToUpper(ev.Type) != strings.ToUpper(name) {
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
