package clickhouse_test

import (
	"context"
	"testing"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
)

// TestCDRBatchesAreInsertedAsyncButDurable: a poll batch of CDRs is buffered and merged server-side with every
// other writer's (step-287m), and still only returns once written — the Kafka offset is committed after it.
// A single-row Insert, on the rare paths, stays synchronous.
func TestCDRBatchesAreInsertedAsyncButDurable(t *testing.T) {
	conn, err := clickhouse.NewConn(chtest.Config(t))
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	writer := clickhouse.NewCDRWriter(conn)
	reader := clickhouse.NewCDRReader(conn)

	row := func() clickhouse.CDRRow {
		return clickhouse.CDRRow{
			MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
			Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "22507000000",
			SubmittedAt: time.Now().UTC().Truncate(time.Millisecond), SegmentCount: 1,
			Encoding: clickhouse.EncodingGSM7, Status: clickhouse.StatusAccepted,
		}
	}

	batchID, singleID := "287m-batch-"+uuid.NewString(), "287m-single-"+uuid.NewString()
	batch := []clickhouse.CDRRow{row(), row()}
	if err := writer.InsertBatch(chgo.Context(context.Background(), chgo.WithQueryID(batchID)), batch); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	for _, r := range batch {
		if _, found, err := reader.Current(context.Background(), r.CustomerID, r.AccountID, r.MessageID); err != nil || !found {
			t.Fatalf("row %s right after InsertBatch: found=%v err=%v, want it written before the call returned", r.MessageID, found, err)
		}
	}
	if err := writer.Insert(chgo.Context(context.Background(), chgo.WithQueryID(singleID)), row()); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	if err := conn.Exec(context.Background(), "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	// query_log keeps only the settings that differ from the default: wait_for_async_insert=1 is the default and
	// shows up only as an absent key, so the check is that it was not turned off.
	settings := func(queryID string) (inserts uint64, async uint64) {
		t.Helper()
		if err := conn.QueryRow(context.Background(), `SELECT count(),
			countIf(Settings['async_insert'] = '1' AND Settings['wait_for_async_insert'] != '0')
			FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND query_id = ?`,
			queryID).Scan(&inserts, &async); err != nil {
			t.Fatalf("read query_log: %v", err)
		}
		return inserts, async
	}
	if inserts, async := settings(batchID); inserts != 2 || async != 2 {
		t.Errorf("InsertBatch: %d inserts, %d async with wait, want the cdr and cdr_events inserts both async with wait", inserts, async)
	}
	if inserts, async := settings(singleID); inserts != 2 || async != 0 {
		t.Errorf("Insert: %d inserts, %d async, want both synchronous", inserts, async)
	}
}
