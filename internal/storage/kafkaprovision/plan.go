// Package kafkaprovision decides and applies the partition layout of the pipeline's Kafka topics
// (step-201, D7).
//
// It exists because KAFKA_TOPIC_PARTITIONS would otherwise be a dead knob: nothing in the repository
// creates a topic outside the test harness, and a broker's auto-creation gives one partition — so a
// load run would measure an inter-pod parallelism of 1 and blame the gateway for the ceiling.
//
// Two rules shape the whole package:
//
//   - It never runs at a service's boot. Provisioning is a deliberate operator act, like a migration:
//     replicas racing each other at startup, partial failure during a rollout, and nobody owning a
//     topic are the reasons D7 refuses that variant.
//   - It only ever extends. Kafka cannot remove partitions from a topic, so a configuration asking for
//     fewer than a topic already has is refused loudly — never applied, never silently ignored — and
//     the refusal happens before anything is mutated, so a run either applies its whole plan or none
//     of it.
package kafkaprovision

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// Topics returns every topic the provisioner owns, sorted. It is the full registry declared in
// internal/storage/kafka, not the shorter list the test harness pre-creates: a topic missing here is a
// topic whose first producer would find nothing, or whatever a broker auto-creates.
func Topics() []string {
	topics := []string{
		kafka.TopicMTInbound,
		kafka.TopicMTRouted,
		kafka.TopicMTOutcome,
		kafka.TopicMTReroutePark,
		kafka.TopicMTDeadLetter,
		kafka.TopicMOInbound,
		kafka.TopicMORouted,
		kafka.TopicMODeadLetter,
		kafka.TopicDLREvents,
		kafka.TopicDLRDeadLetter,
		kafka.TopicWebhookRetry,
		kafka.TopicWebhookDeadLetter,
		kafka.TopicMetricsStream,
	}
	slices.Sort(topics)
	return topics
}

// Config is the desired layout, straight from the KAFKA_TOPIC_* levers (config.Kafka).
type Config struct {
	// Partitions is the width every topic gets unless Overrides names it.
	Partitions int32
	// Overrides is topic → partition count, as parsed by config.Kafka.PartitionOverrides. That helper
	// deliberately does not check topic names — internal/storage/kafka imports internal/config, so the
	// reverse would be an import cycle — which is why BuildPlan refuses an override naming a topic this
	// package does not own. Otherwise a typo is accepted by config and silently ignored here.
	Overrides map[string]int32
	// ReplicationFactor is applied to topics this run creates. It cannot change an existing topic's
	// replication (that needs a partition reassignment), so a mismatch is reported as a warning.
	ReplicationFactor int16
}

// TopicState is what a topic looks like on the cluster right now.
type TopicState struct {
	Partitions        int32
	ReplicationFactor int16
}

// Action is what a run would do to one topic.
type Action int

const (
	// ActionUnchanged means the topic already has at least the requested width.
	ActionUnchanged Action = iota
	// ActionCreate means the topic does not exist.
	ActionCreate
	// ActionExpand means the topic exists with fewer partitions than requested.
	ActionExpand
)

// String renders the action for an operator's console.
func (a Action) String() string {
	switch a {
	case ActionCreate:
		return "CREATE"
	case ActionExpand:
		return "EXPAND"
	case ActionUnchanged:
		return "OK"
	default:
		return fmt.Sprintf("Action(%d)", int(a))
	}
}

// Change is one topic's line of the plan. Have is 0 when the topic is absent.
type Change struct {
	Topic  string
	Action Action
	Have   int32
	Want   int32
}

// String renders one plan line, e.g. "EXPAND mt.routed 4 -> 12 partitions".
func (c Change) String() string {
	if c.Action == ActionCreate {
		return fmt.Sprintf("%-6s %-20s -> %d partitions", c.Action, c.Topic, c.Want)
	}
	return fmt.Sprintf("%-6s %-20s %d -> %d partitions", c.Action, c.Topic, c.Have, c.Want)
}

// Plan is what a run would do, decided in full before anything is mutated.
type Plan struct {
	// Changes holds one entry per owned topic, sorted by topic name. A refused plan carries none.
	Changes []Change
	// Warnings are divergences this tool cannot fix, worth saying out loud rather than leaving invisible.
	Warnings []string
}

// Pending counts the topics a run would actually touch.
func (p Plan) Pending() int {
	n := 0
	for _, change := range p.Changes {
		if change.Action != ActionUnchanged {
			n++
		}
	}
	return n
}

// BuildPlan decides what to do from the desired config and the cluster's current state. current maps
// topic → state and omits absent topics.
//
// It returns an error — and an empty plan — when the config is nonsense, when an override names a topic
// this package does not own, or when any topic would have to shrink. Every offending topic is named at
// once: a run refused twice for one typo at a time wastes a maintenance window.
func BuildPlan(cfg Config, current map[string]TopicState) (Plan, error) {
	if cfg.Partitions < 1 {
		return Plan{}, fmt.Errorf("partition count %d must be at least 1", cfg.Partitions)
	}
	if cfg.ReplicationFactor < 1 {
		return Plan{}, fmt.Errorf("replication factor %d must be at least 1 "+
			"(the broker default sentinel -1 is not accepted: the data plane's durability is set here)",
			cfg.ReplicationFactor)
	}

	owned := Topics()
	var unknown []string
	for topic := range cfg.Overrides {
		if !slices.Contains(owned, topic) {
			unknown = append(unknown, topic)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return Plan{}, fmt.Errorf(
			"partition override names unknown topic(s) %s: no such topic is declared in internal/storage/kafka "+
				"(known topics: %s)",
			quoteAll(unknown), strings.Join(owned, ", "))
	}

	var (
		plan    Plan
		shrinks []string
	)
	for _, topic := range owned {
		want := cfg.Partitions
		if override, ok := cfg.Overrides[topic]; ok {
			want = override
		}

		state, exists := current[topic]
		if !exists {
			plan.Changes = append(plan.Changes, Change{Topic: topic, Action: ActionCreate, Want: want})
			continue
		}

		switch {
		case state.Partitions > want:
			shrinks = append(shrinks, fmt.Sprintf("%s has %d partitions, more than the %d requested",
				topic, state.Partitions, want))
		case state.Partitions < want:
			plan.Changes = append(plan.Changes, Change{
				Topic: topic, Action: ActionExpand, Have: state.Partitions, Want: want,
			})
		default:
			plan.Changes = append(plan.Changes, Change{
				Topic: topic, Action: ActionUnchanged, Have: state.Partitions, Want: want,
			})
		}

		if state.ReplicationFactor != cfg.ReplicationFactor {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf(
				"%s has replication factor %d, not the %d configured — this tool cannot change it "+
					"(a partition reassignment can); the topic keeps the durability it was created with",
				topic, state.ReplicationFactor, cfg.ReplicationFactor))
		}
	}

	if len(shrinks) > 0 {
		return Plan{}, fmt.Errorf(
			"refusing to shrink: %s. Kafka cannot remove partitions from a topic; raise the configuration "+
				"back or recreate the topic deliberately", strings.Join(shrinks, "; "))
	}
	return plan, nil
}

func quoteAll(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return strings.Join(quoted, ", ")
}
