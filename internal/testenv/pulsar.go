//go:build integration

package testenv

import (
	"context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	tcpulsar "github.com/testcontainers/testcontainers-go/modules/pulsar"
)

// Pin the broker rather than tracking latest: a test tier that changes
// behaviour when an upstream tag moves is not a test tier.
const pulsarImage = "apachepulsar/pulsar:4.2.4"

// Pulsar is a running broker and a client connected to it.
type Pulsar struct {
	URL    string
	Client pulsar.Client
}

// StartPulsar brings up a broker for the duration of the test.
func StartPulsar(t *testing.T) *Pulsar {
	t.Helper()
	ctx := context.Background()

	container, err := tcpulsar.Run(ctx, pulsarImage)
	if err != nil {
		t.Fatalf("start pulsar: %v\nis Docker running?", err)
	}
	t.Cleanup(func() { container.Terminate(context.Background()) })

	url, err := container.BrokerURL(ctx)
	if err != nil {
		t.Fatalf("pulsar broker url: %v", err)
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               url,
		ConnectionTimeout: 15 * time.Second,
		OperationTimeout:  30 * time.Second,
		Logger:            plog.DefaultNopLogger(),
	})
	if err != nil {
		t.Fatalf("pulsar client at %s: %v", url, err)
	}
	t.Cleanup(client.Close)

	return &Pulsar{URL: url, Client: client}
}

// Produce publishes payloads to topic in order, blocking until each is
// persisted so a subsequent consumer cannot race ahead of them.
func (p *Pulsar) Produce(t *testing.T, topic string, payloads ...[]byte) {
	t.Helper()
	producer, err := p.Client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		t.Fatalf("create producer on %s: %v", topic, err)
	}
	defer producer.Close()

	ctx := context.Background()
	for i, payload := range payloads {
		if _, err := producer.Send(ctx, &pulsar.ProducerMessage{Payload: payload}); err != nil {
			t.Fatalf("send message %d to %s: %v", i, topic, err)
		}
	}
}

// Subscribe returns a consumer reading topic from the beginning, so a test may
// subscribe after producing without losing what it just published.
func (p *Pulsar) Subscribe(t *testing.T, topic, subscription string) pulsar.Consumer {
	t.Helper()
	consumer, err := p.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subscription,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		t.Fatalf("subscribe %q to %s: %v", subscription, topic, err)
	}
	t.Cleanup(consumer.Close)
	return consumer
}
