//go:build integration

package testenv

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// The same image and flags the local stack runs, pinned: a test tier that
	// changes behaviour when an upstream tag moves is not a test tier.
	pulsarImage       = "apachepulsar/pulsar:4.2.4"
	pulsarServicePort = "6650/tcp"
	pulsarAdminPort   = "8080/tcp"
)

// Pulsar is a running broker and a client connected to it.
type Pulsar struct {
	URL    string
	Client pulsar.Client
}

// StartPulsar brings up a broker for the duration of the test.
func StartPulsar(t *testing.T) *Pulsar {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        pulsarImage,
		ExposedPorts: []string{pulsarServicePort, pulsarAdminPort},
		// -nfw/-nss drop the functions worker and stream storage: neither is
		// used here and both add startup time.
		Cmd: []string{"bin/pulsar", "standalone", "-nfw", "-nss"},
		// Standalone Pulsar sizes its heap for a server by default, which is
		// wasteful for a test broker and slow to start.
		Env: map[string]string{
			"PULSAR_MEM": "-Xms256m -Xmx256m -XX:MaxDirectMemorySize=256m",
		},
		// Both listening ports and the "messaging service is ready" log show
		// up seconds before the standalone cluster's metadata exists, and a
		// producer created in that window fails with TopicNotFound. The admin
		// API naming the cluster is the first signal the namespace can
		// actually be used.
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(pulsarServicePort),
			wait.ForHTTP("/admin/v2/clusters").
				WithPort(pulsarAdminPort).
				WithResponseMatcher(func(r io.Reader) bool {
					body, err := io.ReadAll(r)
					return err == nil && strings.TrimSpace(string(body)) == `["standalone"]`
				}),
		).WithDeadline(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start pulsar: %v\nis Docker running?", err)
	}
	t.Cleanup(func() { container.Terminate(context.Background()) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("pulsar host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6650")
	if err != nil {
		t.Fatalf("pulsar mapped port: %v", err)
	}
	url := fmt.Sprintf("pulsar://%s:%s", host, port.Port())

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
