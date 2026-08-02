package kafkaprovision_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/kafkaprovision"
)

// createCall and updateCall record what the provisioner asked the broker to do, so a test can prove
// what was NOT asked — the half that matters when a plan is refused.
type createCall struct {
	partitions  int32
	replication int16
	topics      []string
}

type updateCall struct {
	set    int
	topics []string
}

type fakeAdmin struct {
	state     map[string]kafkaprovision.TopicState
	listErr   error
	createErr map[string]error // per topic, returned in the response like a broker does

	creates []createCall
	updates []updateCall
}

func (f *fakeAdmin) ListTopics(_ context.Context, topics ...string) (kadm.TopicDetails, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	details := make(kadm.TopicDetails, len(topics))
	for _, topic := range topics {
		state, ok := f.state[topic]
		if !ok {
			details[topic] = kadm.TopicDetail{Topic: topic, Err: kerr.UnknownTopicOrPartition}
			continue
		}
		partitions := make(kadm.PartitionDetails, state.Partitions)
		for i := int32(0); i < state.Partitions; i++ {
			replicas := make([]int32, state.ReplicationFactor)
			partitions[i] = kadm.PartitionDetail{Topic: topic, Partition: i, Replicas: replicas}
		}
		details[topic] = kadm.TopicDetail{Topic: topic, Partitions: partitions}
	}
	return details, nil
}

func (f *fakeAdmin) CreateTopics(_ context.Context, partitions int32, replication int16, _ map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
	f.creates = append(f.creates, createCall{partitions: partitions, replication: replication, topics: slices.Clone(topics)})
	resp := make(kadm.CreateTopicResponses, len(topics))
	for _, topic := range topics {
		resp[topic] = kadm.CreateTopicResponse{Topic: topic, Err: f.createErr[topic]}
	}
	return resp, nil
}

func (f *fakeAdmin) UpdatePartitions(_ context.Context, set int, topics ...string) (kadm.CreatePartitionsResponses, error) {
	f.updates = append(f.updates, updateCall{set: set, topics: slices.Clone(topics)})
	resp := make(kadm.CreatePartitionsResponses, len(topics))
	for _, topic := range topics {
		resp[topic] = kadm.CreatePartitionsResponse{Topic: topic}
	}
	return resp, nil
}

// fullState is every declared topic already at n partitions with rf replicas.
func fullState(n int32, rf int16) map[string]kafkaprovision.TopicState {
	state := make(map[string]kafkaprovision.TopicState, len(kafkaprovision.Topics()))
	for _, topic := range kafkaprovision.Topics() {
		state[topic] = kafkaprovision.TopicState{Partitions: n, ReplicationFactor: rf}
	}
	return state
}

func TestProvisionCreatesAbsentAndExpandsShort(t *testing.T) {
	adm := &fakeAdmin{state: map[string]kafkaprovision.TopicState{
		kafka.TopicMTInbound: {Partitions: 12, ReplicationFactor: 1},
		kafka.TopicMTRouted:  {Partitions: 4, ReplicationFactor: 1},
	}}

	plan, err := kafkaprovision.Provision(context.Background(), adm,
		kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("Provision() error = %v, want nil", err)
	}

	var created []string
	for _, call := range adm.creates {
		if call.partitions != 12 {
			t.Errorf("CreateTopics partitions = %d, want 12", call.partitions)
		}
		if call.replication != 1 {
			t.Errorf("CreateTopics replication = %d, want 1", call.replication)
		}
		created = append(created, call.topics...)
	}
	if len(created) != len(kafkaprovision.Topics())-2 {
		t.Errorf("created %d topics (%v), want %d", len(created), created, len(kafkaprovision.Topics())-2)
	}
	if slices.Contains(created, kafka.TopicMTInbound) || slices.Contains(created, kafka.TopicMTRouted) {
		t.Errorf("created = %v, want it to skip the topics that already exist", created)
	}

	if len(adm.updates) != 1 {
		t.Fatalf("len(updates) = %d, want 1: %+v", len(adm.updates), adm.updates)
	}
	if adm.updates[0].set != 12 || !slices.Equal(adm.updates[0].topics, []string{kafka.TopicMTRouted}) {
		t.Errorf("update = %+v, want set 12 on [%s]", adm.updates[0], kafka.TopicMTRouted)
	}
	if plan.Pending() != len(kafkaprovision.Topics())-1 {
		t.Errorf("plan.Pending() = %d, want %d", plan.Pending(), len(kafkaprovision.Topics())-1)
	}
}

// TestProvisionIsIdempotent: re-running against a conforming cluster must touch nothing at all — not
// "create and swallow the error", which would hide a wrong partition count.
func TestProvisionIsIdempotent(t *testing.T) {
	adm := &fakeAdmin{state: fullState(12, 3)}

	plan, err := kafkaprovision.Provision(context.Background(), adm,
		kafkaprovision.Config{Partitions: 12, ReplicationFactor: 3})
	if err != nil {
		t.Fatalf("Provision() error = %v, want nil", err)
	}
	if len(adm.creates) != 0 || len(adm.updates) != 0 {
		t.Errorf("creates = %+v, updates = %+v, want neither", adm.creates, adm.updates)
	}
	if plan.Pending() != 0 {
		t.Errorf("plan.Pending() = %d, want 0", plan.Pending())
	}
}

// TestProvisionAppliesNothingWhenPlanIsRefused is the safety half of "extension only": a refused plan
// must not have half-applied the topics that happened to be fine.
func TestProvisionAppliesNothingWhenPlanIsRefused(t *testing.T) {
	state := fullState(24, 1)
	delete(state, kafka.TopicMORouted) // an absent topic the run would otherwise create

	adm := &fakeAdmin{state: state}
	_, err := kafkaprovision.Provision(context.Background(), adm,
		kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1})
	if err == nil {
		t.Fatalf("Provision() error = nil, want a shrink refusal")
	}
	if len(adm.creates) != 0 || len(adm.updates) != 0 {
		t.Errorf("creates = %+v, updates = %+v, want neither after a refusal", adm.creates, adm.updates)
	}
}

func TestProvisionRefusesUnknownOverrideBeforeTouchingTheCluster(t *testing.T) {
	adm := &fakeAdmin{state: map[string]kafkaprovision.TopicState{}}

	_, err := kafkaprovision.Provision(context.Background(), adm, kafkaprovision.Config{
		Partitions:        12,
		Overrides:         map[string]int32{"mt.inbund": 48},
		ReplicationFactor: 1,
	})
	if err == nil {
		t.Fatalf("Provision() error = nil, want a refusal naming mt.inbund")
	}
	if !strings.Contains(err.Error(), "mt.inbund") {
		t.Errorf("Provision() error = %q, want it to name mt.inbund", err)
	}
	if len(adm.creates) != 0 || len(adm.updates) != 0 {
		t.Errorf("creates = %+v, updates = %+v, want neither", adm.creates, adm.updates)
	}
}

// TestProvisionToleratesConcurrentCreate: two operators running the tool at once must not make one of
// them fail. TOPIC_ALREADY_EXISTS means the topic is there, which is the outcome asked for.
func TestProvisionToleratesConcurrentCreate(t *testing.T) {
	adm := &fakeAdmin{
		state:     map[string]kafkaprovision.TopicState{},
		createErr: map[string]error{kafka.TopicMTInbound: kerr.TopicAlreadyExists},
	}

	if _, err := kafkaprovision.Provision(context.Background(), adm,
		kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1}); err != nil {
		t.Errorf("Provision() error = %v, want nil on TOPIC_ALREADY_EXISTS", err)
	}
}

func TestProvisionSurfacesCreateFailure(t *testing.T) {
	adm := &fakeAdmin{
		state:     map[string]kafkaprovision.TopicState{},
		createErr: map[string]error{kafka.TopicMTInbound: kerr.InvalidReplicationFactor},
	}

	_, err := kafkaprovision.Provision(context.Background(), adm,
		kafkaprovision.Config{Partitions: 12, ReplicationFactor: 9})
	if err == nil {
		t.Fatalf("Provision() error = nil, want the broker's INVALID_REPLICATION_FACTOR")
	}
	if !strings.Contains(err.Error(), kafka.TopicMTInbound) {
		t.Errorf("Provision() error = %q, want it to name the topic that failed", err)
	}
}

func TestProvisionSurfacesMetadataFailure(t *testing.T) {
	sentinel := errors.New("dial tcp: connection refused")
	adm := &fakeAdmin{listErr: sentinel}

	if _, err := kafkaprovision.Provision(context.Background(), adm,
		kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1}); !errors.Is(err, sentinel) {
		t.Errorf("Provision() error = %v, want it to wrap %v", err, sentinel)
	}
}

// TestDryRunTouchesNothing: the plan an operator reads before a maintenance window must not be the act
// itself.
func TestDryRunTouchesNothing(t *testing.T) {
	adm := &fakeAdmin{state: map[string]kafkaprovision.TopicState{
		kafka.TopicMTRouted: {Partitions: 4, ReplicationFactor: 1},
	}}

	plan, err := kafkaprovision.DryRun(context.Background(), adm,
		kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("DryRun() error = %v, want nil", err)
	}
	if len(adm.creates) != 0 || len(adm.updates) != 0 {
		t.Errorf("creates = %+v, updates = %+v, want neither from a dry run", adm.creates, adm.updates)
	}
	if plan.Pending() != len(kafkaprovision.Topics()) {
		t.Errorf("plan.Pending() = %d, want %d", plan.Pending(), len(kafkaprovision.Topics()))
	}
}

// TestProvisionGroupsExpansionsByTargetWidth: overrides mean two topics can want different widths, and
// UpdatePartitions sets ONE count for every topic in the call. A single call for both would silently
// give one of them the other's width.
func TestProvisionGroupsExpansionsByTargetWidth(t *testing.T) {
	adm := &fakeAdmin{state: fullState(4, 1)}

	if _, err := kafkaprovision.Provision(context.Background(), adm, kafkaprovision.Config{
		Partitions:        12,
		Overrides:         map[string]int32{kafka.TopicMTRouted: 48},
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("Provision() error = %v, want nil", err)
	}

	got := make(map[string]int)
	for _, call := range adm.updates {
		for _, topic := range call.topics {
			if prev, dup := got[topic]; dup {
				t.Errorf("%s was updated twice (%d then %d)", topic, prev, call.set)
			}
			got[topic] = call.set
		}
	}
	if got[kafka.TopicMTRouted] != 48 {
		t.Errorf("%s was set to %d partitions, want 48", kafka.TopicMTRouted, got[kafka.TopicMTRouted])
	}
	if got[kafka.TopicMTInbound] != 12 {
		t.Errorf("%s was set to %d partitions, want 12", kafka.TopicMTInbound, got[kafka.TopicMTInbound])
	}
}
