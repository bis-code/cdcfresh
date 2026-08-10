// Package canaljson decodes the canal-json wire format that TiCDC emits.
//
// It is named for the format rather than the transport: a Kafka adapter would
// share it with the Pulsar one unchanged.
package canaljson

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bis-code/cdcfresh"
)

// envelope is the subset of canal-json that cdcfresh reads. The fields it
// leaves out are as deliberate as the ones it keeps: sql, sqlType and
// mysqlType are never decoded, because events are doorbells and a value that
// is never read cannot end up in user SQL.
type envelope struct {
	Database string              `json:"database"`
	Table    string              `json:"table"`
	PKNames  []string            `json:"pkNames"`
	IsDdl    bool                `json:"isDdl"`
	Type     string              `json:"type"`
	Data     []map[string]string `json:"data"`
	Old      []map[string]string `json:"old"`

	// canal-json has no commitTs. es is the source event time in
	// milliseconds, which is the closest thing to an ordering token the
	// format offers.
	ES uint64 `json:"es"`
}

// Decode turns one canal-json message into the row events it carries.
//
// A single message can hold several rows — data is an array even when it
// holds one — so this returns a slice and callers must fan out over it.
//
// DDL and watermark messages carry no rows and yield (nil, nil). They are
// routine traffic on a cluster-wide changefeed, not failures. An error means
// the payload could not be understood; the caller should wrap it with
// cdcfresh.ErrSkip, ack the message, and carry on rather than tearing down
// the source.
func Decode(payload []byte) ([]cdcfresh.RowEvent, error) {
	var env envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("canaljson: %w", err)
	}
	if env.IsDdl {
		return nil, nil
	}

	var typ cdcfresh.EventType
	switch strings.ToUpper(env.Type) {
	case "INSERT":
		typ = cdcfresh.Insert
	case "UPDATE":
		typ = cdcfresh.Update
	case "DELETE":
		typ = cdcfresh.Delete
	case "TIDB_WATERMARK":
		return nil, nil
	default:
		// Anything else is dropped loudly rather than quietly: an unfamiliar
		// type is more likely a format change worth noticing than traffic to
		// ignore.
		return nil, fmt.Errorf("canaljson: unrecognised event type %q", env.Type)
	}

	// TiCDC ships a deleted row in data with old null; other canal producers
	// have been known to do the reverse, so accept either.
	rows := env.Data
	if typ == cdcfresh.Delete && len(rows) == 0 {
		rows = env.Old
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("canaljson: %s on %s.%s carries no rows", typ, env.Database, env.Table)
	}

	events := make([]cdcfresh.RowEvent, 0, len(rows))
	for i, row := range rows {
		ev := cdcfresh.RowEvent{
			Database: env.Database,
			Table:    env.Table,
			Type:     typ,
			PKNames:  env.PKNames,
			CommitTs: env.ES,
		}
		if typ == cdcfresh.Delete {
			// RowEvent documents Old as the previous image for a delete, so
			// the removed row moves there regardless of where it arrived.
			ev.Old = values(row)
		} else {
			ev.Data = values(row)
			if i < len(env.Old) {
				ev.Old = values(env.Old[i])
			}
		}
		events = append(events, ev)
	}
	return events, nil
}

// values widens a decoded row to the map[string]any RowEvent carries.
//
// Every canal-json column value is a string, integers included ("value":"10"),
// and they stay strings here. Coercing them would mean guessing a schema the
// decoder does not have, to produce values cdcfresh promises never to put in
// user SQL anyway.
func values(row map[string]string) map[string]any {
	if row == nil {
		return nil
	}
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	return out
}
