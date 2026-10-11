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

// TestReadingAMessageSkipsTheRestOfItsAccount: Current filters on the sort-key prefix (customer_id, account_id), which
// keeps a whole account; without the message_id index (step-287n) a lookup reads every row the account ever wrote.
func TestReadingAMessageSkipsTheRestOfItsAccount(t *testing.T) {
	conn, err := clickhouse.NewConn(chtest.Config(t))
	if err != nil {
		t.Fatalf("new conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx := context.Background()

	const granule, granules = 8192, 4
	customerID, accountID := uuid.New(), uuid.New()
	start := time.Now().UTC().Truncate(time.Millisecond)
	rows := make([]clickhouse.CDRRow, granule*granules)
	for i := range rows {
		rows[i] = clickhouse.CDRRow{
			MessageID: uuid.New(), TraceID: uuid.New(), AccountID: accountID, CustomerID: customerID,
			Direction: clickhouse.DirectionMT, SourceAddr: "GATEWAY", DestAddr: "22507000000",
			SubmittedAt: start.Add(time.Duration(i) * time.Millisecond), SegmentCount: 1,
			Encoding: clickhouse.EncodingGSM7, Status: clickhouse.StatusAccepted,
		}
	}
	if err := clickhouse.NewCDRWriter(conn).InsertBatch(ctx, rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	target := rows[len(rows)/2+granule/2].MessageID
	queryID := "287n-" + uuid.NewString()
	_, found, err := clickhouse.NewCDRReader(conn).Current(chgo.Context(ctx, chgo.WithQueryID(queryID)), customerID, accountID, target)
	if err != nil || !found {
		t.Fatalf("Current: found=%v err=%v", found, err)
	}

	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("flush logs: %v", err)
	}
	var read uint64
	if err := conn.QueryRow(ctx, `SELECT read_rows FROM system.query_log WHERE type = 'QueryFinish' AND query_id = ?`,
		queryID).Scan(&read); err != nil {
		t.Fatalf("read query_log: %v", err)
	}
	// Two granules, not one: a bloom filter answers "maybe" for another granule about once in a hundred.
	if read > 2*granule {
		t.Errorf("Current read %d rows of an account of %d, want at most %d: the lookup scans the account", read, len(rows), 2*granule)
	}
}
