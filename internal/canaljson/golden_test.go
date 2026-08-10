package canaljson

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGoldenFilesAreRealCanalJSON guards the fixtures captured from a live
// TiCDC. They stand in for a running changefeed everywhere else in the test
// suite, so if they stop being valid canal-json the whole tier is testing
// nothing. Checking them needs no container, so it belongs in the unit tier.
func TestGoldenFilesAreRealCanalJSON(t *testing.T) {
	for _, name := range []string{"insert", "update", "delete"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("testdata", name+".json"))
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}

			var ev struct {
				Database string              `json:"database"`
				Table    string              `json:"table"`
				Type     string              `json:"type"`
				IsDdl    bool                `json:"isDdl"`
				PKNames  []string            `json:"pkNames"`
				Data     []map[string]string `json:"data"`
			}
			if err := json.Unmarshal(raw, &ev); err != nil {
				t.Fatalf("not valid canal-json: %v", err)
			}
			if ev.IsDdl {
				t.Errorf("expected a row event, got isDdl=true")
			}
			if ev.Database == "" || ev.Table == "" {
				t.Errorf("missing database/table: %+v", ev)
			}
			if !strings.EqualFold(ev.Type, name) {
				t.Errorf("type = %q, want %q", ev.Type, name)
			}
			// canal-json carries rows as an array even for a single row, so
			// the decoder has to fan out rather than assume one row per
			// message. A fixture that lost this shape would hide that.
			if len(ev.Data) == 0 {
				t.Errorf("data is empty — decoder must fan out over an array")
			}
		})
	}
}
