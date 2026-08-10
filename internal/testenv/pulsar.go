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

	// The namespace every topic in these tests lives in; the broker creates
	// it during startup, after registering itself as a cluster.
	namespace = "public/default"
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
		// Readiness took three attempts to get right, so the measurements are
		// worth recording. Against a cold broker:
		//
		//	port 6650 open          0.0s   <- Docker's proxy, not the broker
		//	/admin/v2/clusters     11.05s
		//	public/default          11.18s  <- what a producer actually needs
		//
		// ForListeningPort is therefore worthless here: the published port
		// answers before anything is behind it. The cluster endpoint looks
		// right but lands 130ms early, and a producer created in that window
		// fails with TopicNotFound — narrow enough to always pass locally and
		// still lose on CI. Topics live in the namespace, so wait for that.
		WaitingFor: wait.ForHTTP("/admin/v2/namespaces/public").
			WithPort(pulsarAdminPort).
			WithResponseMatcher(func(r io.Reader) bool {
				body, err := io.ReadAll(r)
				return err == nil && strings.Contains(string(body), namespace)
			}).
			WithStartupTimeout(2 * time.Minute),
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

	// The readiness probe above closes the window this guards, but only by
	// about 130ms on the machine it was measured on. Auto topic creation is
	// the first thing to touch a freshly-created namespace, so give it a few
	// attempts rather than turning a slow CI runner into a red build.
	var producer pulsar.Producer
	var err error
	for attempt := range 10 {
		producer, err = p.Client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
		if err == nil {
			break
		}
		if attempt == 0 {
			t.Logf("create producer on %s: %v — retrying", topic, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("create producer on %s after retries: %v", topic, err)
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
