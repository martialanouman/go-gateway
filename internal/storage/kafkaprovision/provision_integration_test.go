package kafkaprovision_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/kafkaprovision"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
)

// TestProvisionAgainstBroker runs the tool against a real broker, in the order an operator meets the
// four cases: a topic that does not exist, one that is too narrow, a second run over a conforming
// cluster, and a configuration that would shrink. Only a broker can answer whether Kafka accepts the
// requests — a fake proves the decision, not the act.
//
// The harness pre-creates SEVEN of the twelve topics at four partitions each (kafkatest), so this test
// gets the create/expand mix for free: mo.routed, mo.dead-letter, dlr.dead-letter, webhook.dead-letter
// and webhook.retry are absent.
func TestProvisionAgainstBroker(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	ctx := context.Background()

	cfg := kafkaprovision.Config{
		Partitions:        12,
		Overrides:         map[string]int32{kafka.TopicMTRouted: 16},
		ReplicationFactor: 1, // single-broker container
	}
	want := func(topic string) int32 {
		if topic == kafka.TopicMTRouted {
			return 16
		}
		return 12
	}

	// The fixture is only worth something if it starts from a state the run has to change.
	before := observe(ctx, t, brokers)
	if got := before[kafka.TopicMTRouted]; got != 4 {
		t.Fatalf("fixture: %s starts at %d partitions, want 4 — this test would not exercise an expansion",
			kafka.TopicMTRouted, got)
	}
	if _, exists := before[kafka.TopicMORouted]; exists {
		t.Fatalf("fixture: %s already exists — this test would not exercise a creation", kafka.TopicMORouted)
	}

	t.Run("first run creates the absent topics and widens the short ones", func(t *testing.T) {
		plan, err := provision(ctx, t, brokers, cfg)
		if err != nil {
			t.Fatalf("Provision() error = %v, want nil", err)
		}
		if plan.Pending() != len(kafkaprovision.Topics()) {
			t.Errorf("plan.Pending() = %d, want %d — every topic was either absent or too narrow",
				plan.Pending(), len(kafkaprovision.Topics()))
		}

		after := observe(ctx, t, brokers)
		for _, topic := range kafkaprovision.Topics() {
			if after[topic] != want(topic) {
				t.Errorf("%s has %d partitions on the broker, want %d", topic, after[topic], want(topic))
			}
		}
	})

	t.Run("second run changes nothing", func(t *testing.T) {
		plan, err := provision(ctx, t, brokers, cfg)
		if err != nil {
			t.Fatalf("Provision() error = %v, want nil on a re-run", err)
		}
		if plan.Pending() != 0 {
			t.Errorf("plan.Pending() = %d, want 0 — the cluster already conforms", plan.Pending())
		}
		for _, change := range plan.Changes {
			if change.Action != kafkaprovision.ActionUnchanged {
				t.Errorf("change %+v, want unchanged", change)
			}
		}
	})

	t.Run("a narrower configuration is refused, and nothing moves", func(t *testing.T) {
		shrink := kafkaprovision.Config{Partitions: 8, ReplicationFactor: 1}

		if _, err := provision(ctx, t, brokers, shrink); err == nil {
			t.Fatal("Provision() error = nil, want a refusal: Kafka cannot remove partitions")
		} else if !strings.Contains(err.Error(), "shrink") {
			t.Errorf("Provision() error = %q, want it to say what it refused", err)
		}

		after := observe(ctx, t, brokers)
		for _, topic := range kafkaprovision.Topics() {
			if after[topic] != want(topic) {
				t.Errorf("%s has %d partitions after a refused run, want %d untouched",
					topic, after[topic], want(topic))
			}
		}
	})

	t.Run("an override on an unknown topic is refused before anything is created", func(t *testing.T) {
		// Everything but the typo is the configuration the cluster already satisfies — including the
		// mt.routed override. Drop that and the run would be refused for shrinking mt.routed instead, and
		// this subtest would pass without the unknown-topic check existing at all.
		typo := kafkaprovision.Config{
			Partitions:        12,
			Overrides:         map[string]int32{kafka.TopicMTRouted: 16, "mt.inbund": 48},
			ReplicationFactor: 1,
		}

		_, err := provision(ctx, t, brokers, typo)
		if err == nil {
			t.Fatal("Provision() error = nil, want a refusal naming mt.inbund")
		}
		if !strings.Contains(err.Error(), "mt.inbund") {
			t.Fatalf("Provision() error = %q, want the refusal to be about the unknown topic", err)
		}

		adm := admin(t, brokers)
		details, err := adm.ListTopics(ctx, "mt.inbund")
		if err != nil {
			t.Fatalf("list mt.inbund: %v", err)
		}
		if detail, ok := details["mt.inbund"]; ok && detail.Err == nil {
			t.Errorf("the broker now has a topic mt.inbund with %d partitions, want none", len(detail.Partitions))
		}
	})
}

// TestAdminDoesNotServeStaleTopicMetadata pins the trap this tool fell into: kadm answers metadata
// from a per-client cache (kgo.RequestCachedMetadata, five seconds by default) that caches the ANSWER
// "unknown topic" as readily as a real one. A client that asked about a topic before it existed keeps
// reporting it missing for seconds after the broker created it — long enough for a provisioner to
// decide to create it a second time, or to report a cluster it just filled as still empty.
func TestAdminDoesNotServeStaleTopicMetadata(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	ctx := context.Background()
	const probe = "kafkaprovision.cache-probe"

	adm := admin(t, brokers)

	// Ask before it exists: this is what poisons the cache. Without this read the test proves nothing.
	before, err := adm.ListTopics(ctx, probe)
	if err != nil {
		t.Fatalf("list %s: %v", probe, err)
	}
	if detail, ok := before[probe]; ok && detail.Err == nil {
		t.Fatalf("fixture: %s already exists with %d partitions — nothing here would be cached as missing",
			probe, len(detail.Partitions))
	}

	// Create it through a different client, the way a concurrent operator would.
	resp, err := admin(t, brokers).CreateTopics(ctx, 1, 1, nil, probe)
	if err != nil {
		t.Fatalf("create %s: %v", probe, err)
	}
	if err := resp.Error(); err != nil {
		t.Fatalf("create %s: %v", probe, err)
	}
	t.Cleanup(func() {
		if _, err := admin(t, brokers).DeleteTopics(context.Background(), probe); err != nil {
			t.Logf("cleanup: delete %s: %v", probe, err)
		}
	})

	// Comfortably past the client's metadata floor, nowhere near the 5s default.
	time.Sleep(200 * time.Millisecond)

	after, err := adm.ListTopics(ctx, probe)
	if err != nil {
		t.Fatalf("re-list %s: %v", probe, err)
	}
	detail, ok := after[probe]
	if !ok || detail.Err != nil {
		t.Fatalf("the same client still reports %s as %v — it is answering from a stale metadata cache",
			probe, detail.Err)
	}
	if len(detail.Partitions) != 1 {
		t.Errorf("%s has %d partitions, want 1", probe, len(detail.Partitions))
	}
}

// provision runs the tool the way an operator does: one fresh client per invocation, as a new process
// would have.
func provision(ctx context.Context, t *testing.T, brokers []string, cfg kafkaprovision.Config) (kafkaprovision.Plan, error) {
	t.Helper()
	return kafkaprovision.Provision(ctx, admin(t, brokers), cfg)
}

func admin(t *testing.T, brokers []string) *kadm.Client {
	t.Helper()

	adm, err := kafkaprovision.NewAdmin(brokers, 5*time.Second)
	if err != nil {
		t.Fatalf("NewAdmin(): %v", err)
	}
	t.Cleanup(adm.Close)
	return adm
}

// observe reads the partition count of every owned topic straight from the broker, so an assertion
// checks the cluster and not the tool's own report.
//
// It uses its own client on purpose: kadm answers metadata from a per-client cache that also caches
// "unknown topic", so a client that asked before a topic existed would keep reporting it missing.
func observe(ctx context.Context, t *testing.T, brokers []string) map[string]int32 {
	t.Helper()

	details, err := admin(t, brokers).ListTopics(ctx, kafkaprovision.Topics()...)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	counts := make(map[string]int32, len(details))
	for topic, detail := range details {
		if detail.Err != nil {
			continue
		}
		counts[topic] = int32(len(detail.Partitions))
	}
	return counts
}
