package clickhouse_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
)

func TestCDRVersionedReadWrite(t *testing.T) {
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
	traceID := uuid.New()
	accountID := uuid.New()
	customerID := uuid.New()
	// submitted_at is immutable across a message's rows; both the accepted and enroute rows carry it.
	submittedAt := time.Now().UTC().Truncate(time.Millisecond)

	accepted := clickhouse.CDRRow{
		MessageID:    messageID,
		TraceID:      traceID,
		AccountID:    accountID,
		CustomerID:   customerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   "GATEWAY",
		DestAddr:     "22507000000",
		SubmittedAt:  submittedAt,
		Status:       clickhouse.StatusAccepted,
		SegmentCount: 1,
		Encoding:     clickhouse.EncodingGSM7,
	}
	if err := writer.Insert(ctx, accepted); err != nil {
		t.Fatalf("insert accepted: %v", err)
	}

	// After only the accepted row, the current status is accepted (no 404 window, §1.10).
	got, found, err := reader.Current(ctx, customerID, accountID, messageID)
	if err != nil {
		t.Fatalf("read after accepted: %v", err)
	}
	if !found {
		t.Fatal("accepted message not found")
	}
	if got.Status != clickhouse.StatusAccepted {
		t.Fatalf("status after accepted: got %q want accepted", got.Status)
	}

	// The enroute row (higher lifecycle rank) supersedes accepted, even though it is a new row.
	connectorID := uuid.New()
	enroute := accepted
	enroute.Status = clickhouse.StatusEnroute
	enroute.ConnectorID = &connectorID
	if err := writer.Insert(ctx, enroute); err != nil {
		t.Fatalf("insert enroute: %v", err)
	}

	got, found, err = reader.Current(ctx, customerID, accountID, messageID)
	if err != nil {
		t.Fatalf("read after enroute: %v", err)
	}
	if !found {
		t.Fatal("message not found after enroute")
	}
	if got.Status != clickhouse.StatusEnroute {
		t.Fatalf("status after enroute: got %q want enroute (ReplacingMergeTree should keep the higher version)", got.Status)
	}
	if got.ConnectorID == nil || *got.ConnectorID != connectorID {
		t.Fatalf("connector_id not carried on the enroute row: %v", got.ConnectorID)
	}
	if !got.SubmittedAt.Equal(submittedAt) {
		t.Errorf("submitted_at should be immutable: got %s want %s", got.SubmittedAt, submittedAt)
	}
}

// TestCDRInsertBatch writes several accepted rows in one batch (the accepted-writer's hot path) and
// reads each back, proving the multi-row prepare+send lands every row.
func TestCDRInsertBatch(t *testing.T) {
	cfg := chtest.Config(t)
	conn, err := clickhouse.NewConn(cfg)
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()

	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)
	ctx := context.Background()

	customerID := uuid.New()
	accountID := uuid.New()
	submittedAt := time.Now().UTC().Truncate(time.Millisecond)

	const n = 5
	ids := make([]uuid.UUID, n)
	rows := make([]clickhouse.CDRRow, n)
	for i := range rows {
		ids[i] = uuid.New()
		rows[i] = clickhouse.CDRRow{
			MessageID:    ids[i],
			TraceID:      uuid.New(),
			AccountID:    accountID,
			CustomerID:   customerID,
			Direction:    clickhouse.DirectionMT,
			SourceAddr:   "GATEWAY",
			DestAddr:     "2250700000000",
			SubmittedAt:  submittedAt,
			Status:       clickhouse.StatusAccepted,
			SegmentCount: 1,
			Encoding:     clickhouse.EncodingGSM7,
		}
	}

	if err := writer.InsertBatch(ctx, rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	for i, id := range ids {
		got, found, err := reader.Current(ctx, customerID, accountID, id)
		if err != nil {
			t.Fatalf("read row %d: %v", i, err)
		}
		if !found {
			t.Fatalf("row %d (%s) not found after InsertBatch", i, id)
		}
		if got.Status != clickhouse.StatusAccepted {
			t.Errorf("row %d status: got %q want accepted", i, got.Status)
		}
	}

	// An empty batch is a no-op, not an error.
	if err := writer.InsertBatch(ctx, nil); err != nil {
		t.Errorf("InsertBatch(nil) = %v, want nil (no-op)", err)
	}
}

func TestCDRCurrentScopedToAccount(t *testing.T) {
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
	owner := uuid.New()
	ownerCustomer := uuid.New()
	if err := writer.Insert(ctx, clickhouse.CDRRow{
		MessageID: messageID, TraceID: uuid.New(), AccountID: owner, CustomerID: ownerCustomer,
		Direction: clickhouse.DirectionMT, DestAddr: "22507000000", SubmittedAt: time.Now().UTC(),
		Status: clickhouse.StatusAccepted, SegmentCount: 1, Encoding: clickhouse.EncodingGSM7,
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// A different account querying the same message id must not find it (get-message scoping / 404).
	_, found, err := reader.Current(ctx, uuid.New(), uuid.New(), messageID)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if found {
		t.Fatal("message leaked across account scope")
	}

	// The owner finds it.
	_, found, err = reader.Current(ctx, ownerCustomer, owner, messageID)
	if err != nil {
		t.Fatalf("read owner: %v", err)
	}
	if !found {
		t.Fatal("owner could not read its own message")
	}
}
