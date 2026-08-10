package canaljson

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bis-code/cdcfresh"
)

func golden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".json"))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return b
}

// TestDecodeGoldenRowEvents runs the decoder over payloads captured from a
// live TiCDC rather than hand-written ones, so a wire-format assumption that
// is wrong in production is wrong here too.
func TestDecodeGoldenRowEvents(t *testing.T) {
	tests := []struct {
		golden   string
		wantType cdcfresh.EventType
		wantData map[string]string // nil when the event should carry none
		wantOld  map[string]string
	}{
		{
			golden:   "insert",
			wantType: cdcfresh.Insert,
			wantData: map[string]string{"id": "1", "device": "dev-a", "value": "10"},
		},
		{
			golden:   "update",
			wantType: cdcfresh.Update,
			wantData: map[string]string{"id": "1", "device": "dev-a", "value": "11"},
			wantOld:  map[string]string{"id": "1", "device": "dev-a", "value": "10"},
		},
		{
			// canal-json puts the removed row in data; RowEvent documents Old
			// as the previous image for a delete, so it must land there.
			golden:   "delete",
			wantType: cdcfresh.Delete,
			wantOld:  map[string]string{"id": "1", "device": "dev-a", "value": "11"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.golden, func(t *testing.T) {
			events, err := Decode(golden(t, tt.golden))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if len(events) != 1 {
				t.Fatalf("got %d events, want 1", len(events))
			}
			ev := events[0]

			if ev.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", ev.Type, tt.wantType)
			}
			if ev.Database != "demo" || ev.Table != "readings" {
				t.Errorf("got %s.%s, want demo.readings", ev.Database, ev.Table)
			}
			if len(ev.PKNames) != 1 || ev.PKNames[0] != "id" {
				t.Errorf("PKNames = %v, want [id]", ev.PKNames)
			}
			if ev.CommitTs == 0 {
				t.Error("CommitTs = 0, want the source es timestamp")
			}
			assertRow(t, "Data", ev.Data, tt.wantData)
			assertRow(t, "Old", ev.Old, tt.wantOld)
		})
	}
}

func assertRow(t *testing.T, field string, got map[string]any, want map[string]string) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Errorf("%s = %v, want nil", field, got)
		}
		return
	}
	if len(got) != len(want) {
		t.Errorf("%s has %d columns, want %d: %v", field, len(got), len(want), got)
	}
	for col, wantVal := range want {
		// Values stay strings: the decoder has no schema and cdcfresh never
		// passes them to SQL, so coercing would be guessing for no gain.
		gotVal, ok := got[col].(string)
		if !ok {
			t.Errorf("%s[%q] = %#v, want string %q", field, col, got[col], wantVal)
			continue
		}
		if gotVal != wantVal {
			t.Errorf("%s[%q] = %q, want %q", field, col, gotVal, wantVal)
		}
	}
}

// TestDecodeDropsNonRowMessages covers the traffic a cluster-wide changefeed
// carries that is not a row change. Dropping it is normal, so it must not
// surface as an error the caller has to classify.
func TestDecodeDropsNonRowMessages(t *testing.T) {
	watermark := []byte(`{"id":0,"database":"","table":"","pkNames":null,"isDdl":false,` +
		`"type":"TIDB_WATERMARK","es":1786358942663,"ts":1786358942868}`)

	for _, tt := range []struct {
		name    string
		payload []byte
	}{
		{"ddl_create", golden(t, "ddl_create")},
		{"ddl_query", golden(t, "ddl_query")},
		{"watermark", watermark},
	} {
		t.Run(tt.name, func(t *testing.T) {
			events, err := Decode(tt.payload)
			if err != nil {
				t.Fatalf("Decode: %v, want it dropped without error", err)
			}
			if len(events) != 0 {
				t.Errorf("got %d events, want none", len(events))
			}
		})
	}
}

// TestDecodeFansOutRows is the case a single-row fixture cannot cover: one
// message carrying several rows must become several events, or a batched
// change silently dirties only its first key.
func TestDecodeFansOutRows(t *testing.T) {
	payload := []byte(`{"database":"demo","table":"readings","pkNames":["id"],"isDdl":false,` +
		`"type":"INSERT","es":1786358942713,"old":null,"data":[` +
		`{"id":"1","device":"dev-a","value":"10"},` +
		`{"id":"2","device":"dev-b","value":"20"},` +
		`{"id":"3","device":"dev-c","value":"30"}]}`)

	events, err := Decode(payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	for i, want := range []string{"dev-a", "dev-b", "dev-c"} {
		if got := events[i].Data["device"]; got != want {
			t.Errorf("event %d device = %v, want %q", i, got, want)
		}
	}
}

// TestDecodeUpdatePairsRowsByIndex guards the pairing: a batched update whose
// old images drift out of alignment would attribute the wrong previous state
// to a row.
func TestDecodeUpdatePairsRowsByIndex(t *testing.T) {
	payload := []byte(`{"database":"demo","table":"readings","pkNames":["id"],"isDdl":false,` +
		`"type":"UPDATE","es":1,"old":[{"id":"1","value":"10"},{"id":"2","value":"20"}],` +
		`"data":[{"id":"1","value":"11"},{"id":"2","value":"21"}]}`)

	events, err := Decode(payload)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	for i, want := range []struct{ old, new string }{{"10", "11"}, {"20", "21"}} {
		if got := events[i].Old["value"]; got != want.old {
			t.Errorf("event %d Old[value] = %v, want %q", i, got, want.old)
		}
		if got := events[i].Data["value"]; got != want.new {
			t.Errorf("event %d Data[value] = %v, want %q", i, got, want.new)
		}
	}
}

// TestDecodeRejectsUnusable covers what must become an ErrSkip at the adapter
// rather than being passed off as an empty result.
func TestDecodeRejectsUnusable(t *testing.T) {
	for _, tt := range []struct {
		name, payload, wantErr string
	}{
		{"not json", `{"database":`, "canaljson"},
		{"unknown type", `{"isDdl":false,"type":"BANANA","data":[{"id":"1"}]}`, "unrecognised event type"},
		{"row event with no rows", `{"isDdl":false,"type":"INSERT","data":[]}`, "carries no rows"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			events, err := Decode([]byte(tt.payload))
			if err == nil {
				t.Fatalf("Decode returned %d events and no error, want an error", len(events))
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}
