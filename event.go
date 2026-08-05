package cdcfresh

import "context"

// Key identifies one dirty scope of a derived table. It is opaque to the
// library: encode any structure you need ("device:123") and parse it only
// inside your own Rebuild.
type Key string

// EventType classifies a row change.
type EventType uint8

const (
	Insert EventType = iota + 1
	Update
	Delete
)

// RowEvent is a decoded change event — a doorbell, never data: cdcfresh
// forwards it to Scope and nothing else.
type RowEvent struct {
	Database string
	Table    string
	Type     EventType
	PKNames  []string
	Data     map[string]any // new row image (Insert/Update)
	Old      map[string]any // previous row image (Update/Delete)
	CommitTs uint64
}

// Event pairs a RowEvent with its acknowledgement handle. Ack (if non-nil)
// is called by the core once the event's keys are enqueued — before, and
// independent of, any rebuild.
type Event struct {
	Row RowEvent
	Ack func()
}

// EventSource delivers decoded events. Receive blocks until an event is
// available or ctx is done. Errors are fatal to Run: adapters own
// transient-failure retry internally.
type EventSource interface {
	Receive(ctx context.Context) (Event, error)
}

// ScopeFunc maps a row event to the derived-table scopes it dirties.
type ScopeFunc func(RowEvent) []Key

// RebuildFunc recomputes one scope from the source tables. It must be
// idempotent: at-least-once delivery makes duplicate invocations routine.
type RebuildFunc func(context.Context, Key) error

// EnumerateFunc lists the live key universe for a reconcile sweep.
type EnumerateFunc func(context.Context) ([]Key, error)
