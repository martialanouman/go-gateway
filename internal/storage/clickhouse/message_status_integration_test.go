package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
)

// TestMessageStatusResolvesLatestVersion proves the reaper's outcome read (step-190) collapses the CDR to
// its CURRENT status. The cdr table is a ReplacingMergeTree carrying a lifecycle `version`, so a message
// has one row per stage: reading without resolving on the highest version would hand the reaper the
// initial `accepted` of a message already delivered — and the reaper would then leave a settled-in-fact
// reservation open forever, or worse, treat a delivered message as still in flight.
func TestMessageStatusResolvesLatestVersion(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	messageID := uuid.New()
	submittedAt := time.Now().UTC().Truncate(time.Millisecond)
	base := clickhouse.CDRRow{
		MessageID:    messageID,
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   "GATEWAY",
		DestAddr:     "22507000000",
		SubmittedAt:  submittedAt,
		SegmentCount: 1,
		Encoding:     clickhouse.EncodingGSM7,
	}

	// Write the lifecycle in order: accepted first, then the terminal delivered.
	accepted := base
	accepted.Status = clickhouse.StatusAccepted
	if err := writer.Insert(ctx, accepted); err != nil {
		t.Fatalf("insert accepted: %v", err)
	}
	delivered := base
	delivered.Status = clickhouse.StatusDelivered
	deliveredAt := submittedAt.Add(2 * time.Second)
	delivered.DeliveredAt = &deliveredAt
	if err := writer.Insert(ctx, delivered); err != nil {
		t.Fatalf("insert delivered: %v", err)
	}

	status, found, err := reader.MessageStatus(ctx, messageID)
	if err != nil {
		t.Fatalf("MessageStatus: %v", err)
	}
	if !found {
		t.Fatal("MessageStatus found=false for a message with two CDR rows")
	}
	if status != string(clickhouse.StatusDelivered) {
		t.Errorf("MessageStatus = %q, want %q — the read did not resolve the highest version",
			status, clickhouse.StatusDelivered)
	}
}

// TestMessageStatusUnknownMessage proves an unknown message reads as not-found rather than an error or an
// empty status. The reaper leans on this: found=false is what makes it leave a reservation intact and
// alert, instead of guessing a settlement.
func TestMessageStatusUnknownMessage(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_, found, err := clickhouse.NewCDRReader(conn).MessageStatus(context.Background(), uuid.New())
	if err != nil {
		t.Fatalf("MessageStatus(unknown) errored: %v", err)
	}
	if found {
		t.Error("MessageStatus(unknown) found=true, want false")
	}
}
