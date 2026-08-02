package kafkaprovision_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/kafkaprovision"
)

// TestTopicsCoversEveryDeclaredTopic guards the drift the whole tool would die of: a topic constant
// added to internal/storage/kafka and forgotten here would never be provisioned, and its first
// producer would silently get whatever the broker auto-creates (one partition) — or nothing.
// kafkatest pre-creates an INCOMPLETE list, so it cannot be the reference; the constants are.
func TestTopicsCoversEveryDeclaredTopic(t *testing.T) {
	declared := declaredTopicConstants(t)
	if len(declared) < 12 {
		t.Fatalf("parsed %d topic constants from internal/storage/kafka/topics.go, want at least 12 — the parser is not reading the file", len(declared))
	}

	provisioned := kafkaprovision.Topics()
	for _, topic := range declared {
		if !slices.Contains(provisioned, topic) {
			t.Errorf("Topics() is missing declared topic %q", topic)
		}
	}
	for _, topic := range provisioned {
		if !slices.Contains(declared, topic) {
			t.Errorf("Topics() provisions %q, which no kafka.Topic* constant declares", topic)
		}
	}
}

// declaredTopicConstants reads the topic names out of the registry's source, so the guard cannot be
// satisfied by copying the same mistake twice.
func declaredTopicConstants(t *testing.T) []string {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), "../kafka/topics.go", nil, 0)
	if err != nil {
		t.Fatalf("parse ../kafka/topics.go: %v", err)
	}

	var topics []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
				continue
			}
			if !strings.HasPrefix(value.Names[0].Name, "Topic") {
				continue
			}
			lit, ok := value.Values[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			name, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatalf("unquote %s = %s: %v", value.Names[0].Name, lit.Value, err)
			}
			topics = append(topics, name)
		}
	}
	return topics
}

func TestBuildPlanClassifiesEveryTopic(t *testing.T) {
	current := map[string]kafkaprovision.TopicState{
		kafka.TopicMTInbound: {Partitions: 12, ReplicationFactor: 1},
		kafka.TopicMTRouted:  {Partitions: 4, ReplicationFactor: 1},
	}

	plan, err := kafkaprovision.BuildPlan(kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1}, current)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v, want nil", err)
	}

	if len(plan.Changes) != len(kafkaprovision.Topics()) {
		t.Fatalf("len(plan.Changes) = %d, want %d (one per topic)", len(plan.Changes), len(kafkaprovision.Topics()))
	}

	byTopic := make(map[string]kafkaprovision.Change, len(plan.Changes))
	for _, change := range plan.Changes {
		byTopic[change.Topic] = change
	}

	if got := byTopic[kafka.TopicMTInbound]; got.Action != kafkaprovision.ActionUnchanged || got.Have != 12 || got.Want != 12 {
		t.Errorf("change for %s = %+v, want unchanged 12->12", kafka.TopicMTInbound, got)
	}
	if got := byTopic[kafka.TopicMTRouted]; got.Action != kafkaprovision.ActionExpand || got.Have != 4 || got.Want != 12 {
		t.Errorf("change for %s = %+v, want expand 4->12", kafka.TopicMTRouted, got)
	}
	// mo.routed is one of the five topics kafkatest never pre-creates: absent means create, not expand.
	if got := byTopic[kafka.TopicMORouted]; got.Action != kafkaprovision.ActionCreate || got.Have != 0 || got.Want != 12 {
		t.Errorf("change for %s = %+v, want create 0->12", kafka.TopicMORouted, got)
	}
}

func TestBuildPlanHonoursPerTopicOverride(t *testing.T) {
	cfg := kafkaprovision.Config{
		Partitions:        12,
		Overrides:         map[string]int32{kafka.TopicMTRouted: 48},
		ReplicationFactor: 3,
	}

	plan, err := kafkaprovision.BuildPlan(cfg, map[string]kafkaprovision.TopicState{})
	if err != nil {
		t.Fatalf("BuildPlan() error = %v, want nil", err)
	}

	for _, change := range plan.Changes {
		want := int32(12)
		if change.Topic == kafka.TopicMTRouted {
			want = 48
		}
		if change.Want != want {
			t.Errorf("Want for %s = %d, want %d", change.Topic, change.Want, want)
		}
	}
}

// TestBuildPlanRefusesUnknownOverrideTopic covers the requirement config cannot cover: PartitionOverrides
// deliberately does not validate topic names (internal/storage/kafka imports internal/config, so the
// reverse would be an import cycle). Without this check, "mt.inbund=48" is accepted by config and
// silently ignored here — the hot topic stays at the default width and nobody knows why.
func TestBuildPlanRefusesUnknownOverrideTopic(t *testing.T) {
	cfg := kafkaprovision.Config{
		Partitions:        12,
		Overrides:         map[string]int32{"mt.inbund": 48, kafka.TopicMTRouted: 24},
		ReplicationFactor: 1,
	}

	plan, err := kafkaprovision.BuildPlan(cfg, map[string]kafkaprovision.TopicState{})
	if err == nil {
		t.Fatalf("BuildPlan() error = nil, want an error naming the unknown topic (plan = %+v)", plan)
	}
	if !strings.Contains(err.Error(), "mt.inbund") {
		t.Errorf("BuildPlan() error = %q, want it to name the unknown topic %q", err, "mt.inbund")
	}
	if strings.Contains(err.Error(), "\""+kafka.TopicMTRouted+"\"") {
		t.Errorf("BuildPlan() error = %q, want it NOT to blame the valid override %q", err, kafka.TopicMTRouted)
	}
	if len(plan.Changes) != 0 {
		t.Errorf("len(plan.Changes) = %d, want 0 — a refused plan must carry nothing to apply", len(plan.Changes))
	}
}

// TestBuildPlanRefusesShrink: Kafka cannot remove partitions. A configuration asking for fewer than a
// topic already has must fail loudly, never be applied and never be quietly ignored.
func TestBuildPlanRefusesShrink(t *testing.T) {
	current := map[string]kafkaprovision.TopicState{
		kafka.TopicMTInbound: {Partitions: 24, ReplicationFactor: 1},
		kafka.TopicMTRouted:  {Partitions: 48, ReplicationFactor: 1},
		kafka.TopicMOInbound: {Partitions: 12, ReplicationFactor: 1},
	}

	plan, err := kafkaprovision.BuildPlan(kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1}, current)
	if err == nil {
		t.Fatalf("BuildPlan() error = nil, want a refusal (plan = %+v)", plan)
	}
	for _, want := range []string{kafka.TopicMTInbound, kafka.TopicMTRouted, "24", "48", "12"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("BuildPlan() error = %q, want it to mention %q", err, want)
		}
	}
	if strings.Contains(err.Error(), kafka.TopicMOInbound) {
		t.Errorf("BuildPlan() error = %q, want it NOT to blame %q, which matches", err, kafka.TopicMOInbound)
	}
	if len(plan.Changes) != 0 {
		t.Errorf("len(plan.Changes) = %d, want 0 — a refused plan must carry nothing to apply", len(plan.Changes))
	}
}

// TestBuildPlanWarnsOnReplicationMismatch: Kafka cannot change a replication factor here (it needs a
// partition reassignment), so an existing topic auto-created at 1 replica stays at 1. Say it instead of
// leaving a durability hole invisible.
func TestBuildPlanWarnsOnReplicationMismatch(t *testing.T) {
	current := map[string]kafkaprovision.TopicState{
		kafka.TopicMTInbound: {Partitions: 12, ReplicationFactor: 1},
		kafka.TopicMTRouted:  {Partitions: 12, ReplicationFactor: 3},
	}

	plan, err := kafkaprovision.BuildPlan(kafkaprovision.Config{Partitions: 12, ReplicationFactor: 3}, current)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v, want nil — a replication mismatch is a warning, not a refusal", err)
	}
	if len(plan.Warnings) != 1 {
		t.Fatalf("len(plan.Warnings) = %d, want 1: %v", len(plan.Warnings), plan.Warnings)
	}
	if !strings.Contains(plan.Warnings[0], kafka.TopicMTInbound) {
		t.Errorf("warning = %q, want it to name %q", plan.Warnings[0], kafka.TopicMTInbound)
	}
}

func TestBuildPlanRefusesNonsenseConfig(t *testing.T) {
	tests := map[string]kafkaprovision.Config{
		"zero partitions":      {Partitions: 0, ReplicationFactor: 1},
		"zero replication":     {Partitions: 12, ReplicationFactor: 0},
		"negative replication": {Partitions: 12, ReplicationFactor: -1},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := kafkaprovision.BuildPlan(cfg, map[string]kafkaprovision.TopicState{}); err == nil {
				t.Errorf("BuildPlan(%+v) error = nil, want a refusal", cfg)
			}
		})
	}
}

func TestPlanPendingReportsOnlyWork(t *testing.T) {
	current := make(map[string]kafkaprovision.TopicState, len(kafkaprovision.Topics()))
	for _, topic := range kafkaprovision.Topics() {
		current[topic] = kafkaprovision.TopicState{Partitions: 12, ReplicationFactor: 1}
	}

	plan, err := kafkaprovision.BuildPlan(kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1}, current)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v, want nil", err)
	}
	if plan.Pending() != 0 {
		t.Errorf("plan.Pending() = %d, want 0 when every topic already matches", plan.Pending())
	}

	current[kafka.TopicMTRouted] = kafkaprovision.TopicState{Partitions: 4, ReplicationFactor: 1}
	delete(current, kafka.TopicMORouted)

	plan, err = kafkaprovision.BuildPlan(kafkaprovision.Config{Partitions: 12, ReplicationFactor: 1}, current)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v, want nil", err)
	}
	if plan.Pending() != 2 {
		t.Errorf("plan.Pending() = %d, want 2 (one create, one expand)", plan.Pending())
	}
}
