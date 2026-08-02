package kafkaprovision

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// metadataMinAge bounds how long the client may answer a topic-metadata question from its own cache.
//
// It is not a tuning knob, it is a correctness one. kadm reads metadata through
// kgo.RequestCachedMetadata, whose default window is 5s, and the cache keeps NEGATIVE answers too: a
// client that asked about a topic before it existed keeps replying "unknown topic" for five seconds
// after the broker created it. A tool that decides what to create from that answer would create a
// topic twice, or report a cluster it just provisioned as still empty. franz-go's floor is 10ms
// (kgo/config.go:346).
const metadataMinAge = 10 * time.Millisecond

// NewAdmin builds the admin client this package expects: seeded on brokers, dialling within timeout,
// and — see metadataMinAge — not answering topic metadata from a stale cache. Close it when done; that
// closes the underlying client too.
// A dialTimeout of zero or less keeps franz-go's own 10s default rather than passing it through:
// kgo.DialTimeout(0) is net.Dialer{Timeout: 0}, which is no dial timeout at all.
func NewAdmin(brokers []string, dialTimeout time.Duration) (*kadm.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.MetadataMinAge(metadataMinAge),
	}
	if dialTimeout > 0 {
		opts = append(opts, kgo.DialTimeout(dialTimeout))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka admin client: %w", err)
	}
	return kadm.NewClient(client), nil
}

// Admin is the slice of *kadm.Client this package uses, declared here so a test can drive the whole
// decision path without a broker. The integration test still runs against a real one: a fake broker
// proves the plan, not that Kafka accepts it.
//
// An implementation must not answer ListTopics from a long-lived cache — build it with NewAdmin.
type Admin interface {
	ListTopics(ctx context.Context, topics ...string) (kadm.TopicDetails, error)
	CreateTopics(ctx context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error)
	// UpdatePartitions sets a topic's FINAL partition count. It is what makes a re-run idempotent:
	// kadm.CreatePartitions adds a delta, so a plan re-applied after a partial failure would add it
	// twice. Both issue the same CreatePartitions request underneath.
	UpdatePartitions(ctx context.Context, set int, topics ...string) (kadm.CreatePartitionsResponses, error)
}

// DryRun reports what Provision would do, without touching the cluster.
func DryRun(ctx context.Context, adm Admin, cfg Config) (Plan, error) {
	current, err := Observe(ctx, adm)
	if err != nil {
		return Plan{}, err
	}
	return BuildPlan(cfg, current)
}

// Provision brings the cluster's topics up to the configured layout and returns the plan it applied.
//
// It is idempotent: a second run against a conforming cluster issues no create and no partition
// change at all — it does not "create and swallow the error", which would hide a topic sitting at the
// wrong width. A plan that would shrink a topic, or an override naming an unknown topic, is refused
// before any mutation, so a run is all-or-nothing.
func Provision(ctx context.Context, adm Admin, cfg Config) (Plan, error) {
	plan, err := DryRun(ctx, adm, cfg)
	if err != nil {
		return Plan{}, err
	}
	if err := apply(ctx, adm, plan, cfg.ReplicationFactor); err != nil {
		return plan, err
	}
	return plan, nil
}

// Observe reads the current state of the owned topics. A topic the broker does not know is simply
// absent from the result; any other per-topic metadata error is fatal, because a plan built on a
// half-read cluster would create topics that already exist.
func Observe(ctx context.Context, adm Admin) (map[string]TopicState, error) {
	owned := Topics()
	details, err := adm.ListTopics(ctx, owned...)
	if err != nil {
		return nil, fmt.Errorf("read topic metadata: %w", err)
	}

	current := make(map[string]TopicState, len(owned))
	for _, topic := range owned {
		detail, listed := details[topic]
		if !listed {
			continue
		}
		if detail.Err != nil {
			if errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
				continue
			}
			return nil, fmt.Errorf("read metadata of topic %s: %w", topic, detail.Err)
		}
		current[topic] = TopicState{
			//nolint:gosec // G115: a partition count is a broker-side int32 to begin with (kmsg).
			Partitions:        int32(len(detail.Partitions)),
			ReplicationFactor: replicationOf(detail),
		}
	}
	return current, nil
}

// replicationOf reads a topic's replication factor off its partitions. It takes the lowest of them:
// a topic where one partition lost a replica is under-replicated, and rounding that up to the healthy
// partitions' count would hide it behind a clean report.
func replicationOf(detail kadm.TopicDetail) int16 {
	lowest := 0
	for i, partition := range detail.Partitions.Sorted() {
		if i == 0 || len(partition.Replicas) < lowest {
			lowest = len(partition.Replicas)
		}
	}
	return int16(lowest)
}

// apply issues the creates and the expansions, grouped by target width. The grouping is not an
// optimisation: CreateTopics and UpdatePartitions take ONE count for every topic in the call, so
// mixing widths in a single call would give a topic the wrong one.
func apply(ctx context.Context, adm Admin, plan Plan, replication int16) error {
	creates := groupByWidth(plan, ActionCreate)
	for _, width := range sortedKeys(creates) {
		resp, err := adm.CreateTopics(ctx, width, replication, nil, creates[width]...)
		if err != nil {
			return fmt.Errorf("create topics %s: %w", strings.Join(creates[width], ", "), err)
		}
		for _, r := range resp.Sorted() {
			// A concurrent run that got there first satisfies the intent, so it is not a failure.
			if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
				return fmt.Errorf("create topic %s with %d partitions: %w", r.Topic, width, r.Err)
			}
		}
	}

	expands := groupByWidth(plan, ActionExpand)
	for _, width := range sortedKeys(expands) {
		resp, err := adm.UpdatePartitions(ctx, int(width), expands[width]...)
		if err != nil {
			return fmt.Errorf("expand topics %s: %w", strings.Join(expands[width], ", "), err)
		}
		for _, r := range resp.Sorted() {
			if r.Err != nil {
				return fmt.Errorf("expand topic %s to %d partitions: %w", r.Topic, width, r.Err)
			}
		}
	}
	return nil
}

func groupByWidth(plan Plan, action Action) map[int32][]string {
	groups := make(map[int32][]string)
	for _, change := range plan.Changes {
		if change.Action == action {
			groups[change.Want] = append(groups[change.Want], change.Topic)
		}
	}
	for width := range groups {
		slices.Sort(groups[width])
	}
	return groups
}

func sortedKeys(groups map[int32][]string) []int32 {
	widths := make([]int32, 0, len(groups))
	for width := range groups {
		widths = append(widths, width)
	}
	sort.Slice(widths, func(i, j int) bool { return widths[i] < widths[j] })
	return widths
}
