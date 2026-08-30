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
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/ciguard"
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
	// The CDR projection's spool (step-201c): the connector pool publishes every send outcome here
	// instead of writing ClickHouse, and that produce is fail-closed — without the topic the pool cannot
	// commit a single message, so every integration test involving a submit stalls.
	kafka.TopicMTOutcome,
	// The realtime metrics feed (M11): the stream producer is best-effort and drops silently on an unknown
	// topic, so a test that forgot this would pass while publishing nothing.
	kafka.TopicMetricsStream,
}

const (
	defaultTopicPartitions = 4
	topicReplication       = 1
	provisionDeadline      = 30 * time.Second

	// envPartitions overrides how wide the test topics are created. It exists because the number of
	// partitions is the router's parallelism: since step-201d the consume loop runs one goroutine per
	// partition, so a curve of throughput against lane count cannot be drawn without moving this
	// (step-201d D11). The default is unchanged, so no existing test sees a different broker.
	envPartitions = "KAFKATEST_PARTITIONS"

	// maxTopicPartitions bounds the override. It is not a broker limit — it is the point past which a
	// value is a typo rather than an intent, on a single-node Redpanda started for one test run.
	maxTopicPartitions = 1024
)

// topicPartitions is how wide the shared broker's topics are created. It is read once, with the
// container, so every test in a run sees the same topology.
func topicPartitions() int32 {
	raw := os.Getenv(envPartitions)
	if raw == "" {
		return defaultTopicPartitions
	}
	// ParseInt with an explicit 32-bit width rather than Atoi: the value ends up in an int32 field, and
	// Atoi would parse into a machine int and silently wrap on the conversion.
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || n < 1 || n > maxTopicPartitions {
		// A malformed override is a typo in a sweep, and silently falling back would file the run under a
		// partition count nobody ran.
		panic(fmt.Sprintf("kafkatest: %s=%q must be an integer in 1..%d", envPartitions, raw, maxTopicPartitions))
	}
	return int32(n)
}

// endpoints are the shared broker's addresses. adminErr is kept INSIDE it, separate from sharedErr,
// on purpose: over forty integration tests depend on Brokers, and a failure to resolve the admin port
// — a port only the load harness reads — must fail AdminAPI and nothing else. A new instrument does
// not get to take the suite hostage.
type endpoints struct {
	seed     string
	admin    string
	adminErr error
}

var (
	once      sync.Once
	shared    endpoints
	sharedErr error
)

// Brokers returns the seed broker list of a shared Redpanda with the pipeline topics created. It
// skips the test when Docker is unavailable or under -short. Do not stop the container; the
// process teardown reclaims it.
func Brokers(t *testing.T) []string {
	t.Helper()
	return []string{ensure(t).seed}
}

// AdminAPI returns the origin of the shared Redpanda's Admin API (port 9644), where the broker serves
// its Prometheus expositions: /public_metrics for the curated redpanda_* series and /metrics for the
// internal vectorized_* ones.
//
// It skips and fails exactly as Brokers does, with one addition: a broker that came up but whose admin
// port could not be resolved fails HERE and leaves Brokers working, because every other test in the
// tree needs the Kafka port and none of them needs this one.
func AdminAPI(t *testing.T) string {
	t.Helper()
	e := ensure(t)
	if e.adminErr != nil {
		t.Fatalf("kafkatest: admin API of shared redpanda: %v", e.adminErr)
	}
	return e.admin
}

func ensure(t *testing.T) endpoints {
	t.Helper()

	if testing.Short() {
		ciguard.Skip(t, "kafkatest: skipped under -short (needs Docker)")
	}
	ciguard.RequireDocker(t)

	once.Do(func() { shared, sharedErr = start() })
	if sharedErr != nil {
		t.Fatalf("kafkatest: start shared redpanda: %v", sharedErr)
	}
	return shared
}

func start() (endpoints, error) {
	ctx, cancel := context.WithTimeout(context.Background(), provisionDeadline)
	defer cancel()

	container, err := redpanda.Run(ctx, image)
	if err != nil {
		return endpoints{}, fmt.Errorf("run container: %w", err)
	}

	seed, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		return endpoints{}, fmt.Errorf("seed broker: %w", err)
	}

	if err := createTopics(ctx, seed); err != nil {
		return endpoints{}, err
	}

	// Recorded, not returned: see endpoints.adminErr.
	admin, adminErr := container.AdminAPIAddress(ctx)
	return endpoints{seed: seed, admin: admin, adminErr: adminErr}, nil
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
	resp, err := adm.CreateTopics(ctx, topicPartitions(), topicReplication, nil, topics...)
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
