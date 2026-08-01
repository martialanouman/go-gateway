package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
)

// TestTimelineSurvivesAMerge is the regression test for the defect step-185 shipped with on its first
// attempt: the timeline was read from `cdr`, which is ReplacingMergeTree(version) and does NOT carry
// `status` in its sorting key. A background merge therefore collapsed a message's stages down to the
// highest version — `enroute` vanished, a rejected message ended with a single stage, and the same
// message answered with four stages then two at a moment the merge scheduler decided.
//
// OPTIMIZE TABLE ... FINAL forces the merge the scheduler would eventually run, so the test fails
// deterministically against the old read path and passes against the append-only events table.
func TestTimelineSurvivesAMerge(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	writer := clickhouse.NewCDRWriter(conn)
	messageID := uuid.New()
	base := clickhouse.CDRRow{
		MessageID: messageID, TraceID: uuid.New(),
		AccountID: uuid.New(), CustomerID: uuid.New(),
		Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "33612345678",
		SubmittedAt: time.Now().UTC(), Encoding: clickhouse.EncodingGSM7, SegmentCount: 1,
	}

	accepted := base
	accepted.Status = clickhouse.StatusAccepted
	enroute := base
	enroute.Status = clickhouse.StatusEnroute
	enroute.SegmentSeq = 1
	delivered := base
	delivered.Status = clickhouse.StatusDelivered
	delivered.SegmentSeq = 1
	deliveredAt := base.SubmittedAt.Add(2 * time.Second)
	delivered.DeliveredAt = &deliveredAt

	for _, row := range []clickhouse.CDRRow{accepted, enroute, delivered} {
		if err := writer.Insert(ctx, row); err != nil {
			t.Fatalf("insert %s: %v", row.Status, err)
		}
	}

	// The merge the scheduler would run on its own, forced so the test is deterministic.
	if err := conn.Exec(ctx, "OPTIMIZE TABLE cdr FINAL"); err != nil {
		t.Fatalf("optimize cdr: %v", err)
	}

	reader := clickhouse.NewCDRReader(conn)
	milestones, err := reader.Timeline(ctx, messageID)
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}

	got := make([]clickhouse.Status, 0, len(milestones))
	for _, m := range milestones {
		got = append(got, m.Status)
	}
	want := []clickhouse.Status{clickhouse.StatusAccepted, clickhouse.StatusEnroute, clickhouse.StatusDelivered}
	if len(got) != len(want) {
		t.Fatalf("timeline after merge = %v, want %v — the intermediate stages were merged away", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("stage %d = %s, want %s", i, got[i], want[i])
		}
	}
}

// TestTimelineDeduplicatesRedeliveredStages: the data plane is at-least-once, so the same stage can be
// written twice. Two identical spans in a trace read as two events that never happened.
func TestTimelineDeduplicatesRedeliveredStages(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	writer := clickhouse.NewCDRWriter(conn)
	messageID := uuid.New()
	row := clickhouse.CDRRow{
		MessageID: messageID, TraceID: uuid.New(),
		AccountID: uuid.New(), CustomerID: uuid.New(),
		Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "33612345678",
		SubmittedAt: time.Now().UTC(), Encoding: clickhouse.EncodingGSM7, SegmentCount: 1,
		Status: clickhouse.StatusEnroute, SegmentSeq: 1,
	}
	for range 3 {
		if err := writer.Insert(ctx, row); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	milestones, err := clickhouse.NewCDRReader(conn).Timeline(ctx, messageID)
	if err != nil {
		t.Fatalf("Timeline: %v", err)
	}
	if len(milestones) != 1 {
		t.Errorf("got %d milestones for one redelivered stage, want 1", len(milestones))
	}
}
