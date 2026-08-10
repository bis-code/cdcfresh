// Package pulsar is a cdcfresh [cdcfresh.EventSource] backed by Apache
// Pulsar, reading the canal-json a TiCDC changefeed produces.
//
// Only this package imports a Pulsar client; the cdcfresh root package stays
// free of it, which CI enforces.
//
//	src, err := pulsar.Source("pulsar://localhost:6650", []string{"persistent://public/default/cdcfresh"})
//	if err != nil {
//		return err
//	}
//	defer src.Close()
//
//	r, err := cdcfresh.New(cdcfresh.Source(src), cdcfresh.Scope(scope), cdcfresh.Rebuild(rebuild))
//
// The subscription is Failover, matching cdcfresh v1's single-active-consumer
// model: run as many instances as you like and exactly one consumes, with the
// rest ready to take over. A takeover is safe because rebuilds recompute from
// the source tables, and the reconcile sweep at Run start heals whatever the
// outgoing instance had in flight.
package pulsar

import (
	"context"
	"fmt"
	"strings"
	"sync"

	apachepulsar "github.com/apache/pulsar-client-go/pulsar"
	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"

	"github.com/bis-code/cdcfresh"
	"github.com/bis-code/cdcfresh/internal/canaljson"
)

// DefaultSubscription is used when WithSubscription is not given.
const DefaultSubscription = "cdcfresh"

// Src is a Pulsar-backed event source. It owns the client it created, so
// Close is the caller's to make: cdcfresh never closes what it did not open.
type Src struct {
	client   apachepulsar.Client
	consumer apachepulsar.Consumer

	// current holds the rows decoded from the message being handed out. It is
	// touched only by Receive, which cdcfresh calls from one goroutine.
	current batch

	closeOnce sync.Once
}

// Option configures a Src.
type Option func(*config)

type config struct {
	subscription string
	logger       pulsarlog.Logger
}

// WithSubscription overrides the subscription name. Instances that should
// fail over to one another must share it.
func WithSubscription(name string) Option {
	return func(c *config) { c.subscription = name }
}

// WithLogger routes the Pulsar client's own logging. It is silent by default:
// a library should not write to a host application's stderr uninvited.
func WithLogger(l pulsarlog.Logger) Option {
	return func(c *config) { c.logger = l }
}

// Source connects to Pulsar and subscribes to topics.
//
// Several topics are multiplexed onto one consumer rather than one consumer
// each. Two changefeeds can touch the same derived-table scope, and only a
// single stream lets cdcfresh coalesce those into one rebuild.
//
// Like cdcfresh.New, it reports every problem with the arguments in one error
// instead of failing on the first.
func Source(url string, topics []string, opts ...Option) (*Src, error) {
	cfg := config{subscription: DefaultSubscription}
	for _, opt := range opts {
		opt(&cfg)
	}

	var missing []string
	if strings.TrimSpace(url) == "" {
		missing = append(missing, "url")
	}
	if len(topics) == 0 {
		missing = append(missing, "topics")
	}
	for _, t := range topics {
		if strings.TrimSpace(t) == "" {
			missing = append(missing, "topics (contains an empty entry)")
			break
		}
	}
	if strings.TrimSpace(cfg.subscription) == "" {
		missing = append(missing, "subscription (WithSubscription given an empty name)")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("cdcfresh/pulsar: %s", strings.Join(missing, ", "))
	}

	clientOpts := apachepulsar.ClientOptions{URL: url}
	if cfg.logger != nil {
		clientOpts.Logger = cfg.logger
	} else {
		clientOpts.Logger = pulsarlog.DefaultNopLogger()
	}
	client, err := apachepulsar.NewClient(clientOpts)
	if err != nil {
		return nil, fmt.Errorf("cdcfresh/pulsar: connect to %s: %w", url, err)
	}

	consumer, err := client.Subscribe(apachepulsar.ConsumerOptions{
		Topics:           topics,
		SubscriptionName: cfg.subscription,
		Type:             apachepulsar.Failover,
		// Earliest matters on a first subscribe: the changefeed may already
		// have produced before this instance existed, and skipping that
		// backlog would leave derived tables stale until the next reconcile.
		SubscriptionInitialPosition: apachepulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("cdcfresh/pulsar: subscribe %q to %v: %w", cfg.subscription, topics, err)
	}

	return &Src{client: client, consumer: consumer}, nil
}

// Receive implements cdcfresh.EventSource.
//
// One Pulsar message can carry several rows, so messages are handed out one
// row at a time and the ack rides on the last of them — the message is
// acknowledged once every key it dirties has been enqueued, never before.
//
// It is not safe for concurrent use; cdcfresh drives it from a single loop.
func (s *Src) Receive(ctx context.Context) (cdcfresh.Event, error) {
	for {
		if ev, ok := s.current.next(); ok {
			return ev, nil
		}

		msg, err := s.consumer.Receive(ctx)
		if err != nil {
			return cdcfresh.Event{}, fmt.Errorf("cdcfresh/pulsar: receive: %w", err)
		}

		rows, err := canaljson.Decode(msg.Payload())
		if err != nil {
			// The adapter holds the ack handle, so it is the only thing that
			// can retire a message it refuses to decode. ErrSkip keeps this
			// per-message failure from tearing down the whole source.
			s.consumer.Ack(msg)
			return cdcfresh.Event{}, fmt.Errorf("%w: cdcfresh/pulsar: %s: %v", cdcfresh.ErrSkip, msg.Topic(), err)
		}
		if len(rows) == 0 {
			// DDL and watermarks: ordinary traffic on a cluster-wide
			// changefeed, not something the caller needs to hear about.
			s.consumer.Ack(msg)
			continue
		}

		s.current = batch{rows: rows, ack: func() { s.consumer.Ack(msg) }}
	}
}

// Close unsubscribes and releases the client. It is safe to call more than
// once.
func (s *Src) Close() error {
	s.closeOnce.Do(func() {
		s.consumer.Close()
		s.client.Close()
	})
	return nil
}

// batch hands out the rows decoded from one message, attaching that message's
// ack to the last of them.
type batch struct {
	rows []cdcfresh.RowEvent
	ack  func()
}

func (b *batch) next() (cdcfresh.Event, bool) {
	if len(b.rows) == 0 {
		return cdcfresh.Event{}, false
	}
	ev := cdcfresh.Event{Row: b.rows[0]}
	b.rows = b.rows[1:]
	if len(b.rows) == 0 {
		// Last row of the message: releasing the ack here is what makes
		// acking mean "every key from this message is enqueued".
		ev.Ack, b.ack = b.ack, nil
	}
	return ev, true
}
