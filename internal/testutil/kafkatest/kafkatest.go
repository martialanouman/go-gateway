// Package kafkatest starts a throwaway Redpanda (a Kafka-compatible broker) for integration tests
// and pre-creates the pipeline topics, so a test exercises the real producer/consumer path against
// a real broker rather than a mock.
//
// The broker is shared across a package's tests; each test isolates itself with its own consumer
// group and message keys, not a fresh broker. Tests skip cleanly when Docker is unavailable or
// under `go test -short`, mirroring pgtest.
package kafkatest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// image must match docker-compose.yml so the test broker and the dev broker cannot drift.
const image = "redpandadata/redpanda:v24.2.18"

// topics are pre-created with several partitions so partition keying (mt.routed keyed by logical
// message id, §7.3) is actually exercised rather than degenerating to a single partition.
var topics = []string{
	kafka.TopicMTInbound, kafka.TopicMTRouted,
	kafka.TopicMOInbound, kafka.TopicDLREvents,
	// Resilience data-plane topics (M7/M8): the connector pool produces reroutes and dead-letters here,
	// so a test exercising fallback/park/replay or dead-lettering needs them pre-created — otherwise the
	// first produce fails UNKNOWN_TOPIC_OR_PARTITION and the pool tears down.
	kafka.TopicMTReroutePark, kafka.TopicMTDeadLetter,
	// The realtime metrics feed (M11): the stream producer is best-effort and drops silently on an unknown
	// topic, so a test that forgot this would pass while publishing nothing.
	kafka.TopicMetricsStream,
}

const (
	topicPartitions   = 4
	topicReplication  = 1
	provisionDeadline = 30 * time.Second
)

var (
	once      sync.Once
	broker    string
	sharedErr error
)

// Brokers returns the seed broker list of a shared Redpanda with the pipeline topics created. It
// skips the test when Docker is unavailable or under -short. Do not stop the container; the
// process teardown reclaims it.
func Brokers(t *testing.T) []string {
	t.Helper()

	if testing.Short() {
		t.Skip("kafkatest: skipped under -short (needs Docker)")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	once.Do(func() { broker, sharedErr = start() })
	if sharedErr != nil {
		t.Fatalf("kafkatest: start shared redpanda: %v", sharedErr)
	}
	return []string{broker}
}

func start() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), provisionDeadline)
	defer cancel()

	container, err := redpanda.Run(ctx, image)
	if err != nil {
		return "", fmt.Errorf("run container: %w", err)
	}

	seed, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		return "", fmt.Errorf("seed broker: %w", err)
	}

	if err := createTopics(ctx, seed); err != nil {
		return "", err
	}
	return seed, nil
}

// createTopics provisions the pipeline topics. A topic that already exists is not an error: the
// container is created once, but being explicit keeps the harness idempotent.
func createTopics(ctx context.Context, seed string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(seed))
	if err != nil {
		return fmt.Errorf("admin client: %w", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopics(ctx, topicPartitions, topicReplication, nil, topics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, r := range resp.Sorted() {
		if r.Err != nil && !isTopicExists(r.Err) {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}

func isTopicExists(err error) bool {
	// kadm surfaces the broker's TOPIC_ALREADY_EXISTS as an error whose message contains it; a
	// string check avoids importing the kerr code set for one comparison.
	return err != nil && strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS")
}
