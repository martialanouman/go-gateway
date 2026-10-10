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

	status, found, err := reader.MessageStatus(ctx, base.CustomerID, &base.AccountID, messageID)
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

	_, found, err := clickhouse.NewCDRReader(conn).MessageStatus(context.Background(), uuid.New(), nil, uuid.New())
	if err != nil {
		t.Fatalf("MessageStatus(unknown) errored: %v", err)
	}
	if found {
		t.Error("MessageStatus(unknown) found=true, want false")
	}
}

// TestMessageStatusReadsWithinTheReservationsAccount: the reaper knows the customer and account of the
// reservation, so the read stays on the sorting-key prefix instead of scanning every CDR (step-287m). A CDR
// of another account is not this reservation's outcome; without an account (deleted, set to NULL by the
// ledger's FK), the read falls back to the message id alone.
func TestMessageStatusReadsWithinTheReservationsAccount(t *testing.T) {
	conn, err := clickhouse.NewConn(chtest.Config(t))
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	row := clickhouse.CDRRow{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "22507000000",
		SubmittedAt: time.Now().UTC().Truncate(time.Millisecond), SegmentCount: 1,
		Encoding: clickhouse.EncodingGSM7, Status: clickhouse.StatusFailed,
	}
	if err := clickhouse.NewCDRWriter(conn).Insert(ctx, row); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if status, found, err := reader.MessageStatus(ctx, row.CustomerID, &row.AccountID, row.MessageID); err != nil || !found || status != string(clickhouse.StatusFailed) {
		t.Errorf("MessageStatus(own account) = (%q, %v, %v), want (failed, true, nil)", status, found, err)
	}
	otherAccount := uuid.New()
	if _, found, err := reader.MessageStatus(ctx, row.CustomerID, &otherAccount, row.MessageID); err != nil || found {
		t.Errorf("MessageStatus(another account) = (found %v, %v), want not found", found, err)
	}
	if _, found, err := reader.MessageStatus(ctx, uuid.New(), &row.AccountID, row.MessageID); err != nil || found {
		t.Errorf("MessageStatus(another customer) = (found %v, %v), want not found", found, err)
	}
	if status, found, err := reader.MessageStatus(ctx, row.CustomerID, nil, row.MessageID); err != nil || !found || status != string(clickhouse.StatusFailed) {
		t.Errorf("MessageStatus(no account) = (%q, %v, %v), want (failed, true, nil) by message id", status, found, err)
	}
}
