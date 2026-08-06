package cdcfresh

import (
	"context"
	"errors"
	"fmt"
)

// Key identifies one dirty scope of a derived table. It is opaque to the
// library: encode any structure you need ("device:123") and parse it only
// inside your own Rebuild.
type Key string

// EventType classifies a row change.
type EventType uint8

const (
	// Unknown is the zero value: an adapter that fails to set Type produces
	// this rather than a misleading concrete kind.
	Unknown EventType = 0
	Insert  EventType = iota
	Update
	Delete
)

// String renders the event type for logs and error messages.
func (t EventType) String() string {
	switch t {
	case Unknown:
		return "unknown"
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	default:
		return fmt.Sprintf("EventType(%d)", uint8(t))
	}
}

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
// available or ctx is done. Errors are fatal to Run unless they wrap
// ErrSkip: adapters own transient-failure retry internally.
type EventSource interface {
	Receive(ctx context.Context) (Event, error)
}

// ErrSkip marks a Receive error as a non-fatal, per-event skip rather than a
// fatal source failure. An adapter wraps a per-event error it cannot recover
// from (e.g. an undecodable message) with it — fmt.Errorf("%w: ...",
// cdcfresh.ErrSkip) — and acks the dropped message itself, since it holds
// the ack handle. The core counts the skip, reports it via OnError, and
// continues consuming.
var ErrSkip = errors.New("cdcfresh: event skipped")

// ScopeFunc maps a row event to the derived-table scopes it dirties.
type ScopeFunc func(RowEvent) []Key

// RebuildFunc recomputes one scope from the source tables. It must be
// idempotent: at-least-once delivery makes duplicate invocations routine.
type RebuildFunc func(context.Context, Key) error

// EnumerateFunc lists the live key universe for a reconcile sweep.
type EnumerateFunc func(context.Context) ([]Key, error)
