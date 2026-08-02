//go:build loadref

package e2e_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
)

// TestCDRWriteCeiling isolates the CDR write path from the pipeline, because the reference run above
// can only say WHERE the queue is, never WHY.
//
// It is a diagnostic, not a gate: it asserts only that the path moves at all, and reports the curve.
// What the curve answers is the one question the reference run leaves open — does the connector pool's
// output rate rise with the concurrency thrown at it (a per-shard serialisation, fixable by widening
// bind_pool_size) or does it plateau whatever the concurrency (a ceiling on the far side of the
// connection, which no client-side lever moves)?
//
// The row is the one connectorpool writes on a submit_sm_resp — the same two tables, the same
// single-row batch, hence the same four round-trips per message.
func TestCDRWriteCeiling(t *testing.T) {
	chCfg := chtest.Config(t)
	chCfg.MaxOpenConns = int(envFloat(t, envCHMaxOpen, 64))
	chCfg.MaxIdleConns = int(envFloat(t, envCHMaxIdle, 32))

	conn, err := clickhouse.NewConn(chCfg)
	if err != nil {
		t.Fatalf("clickhouse conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	writer := clickhouse.NewCDRWriter(conn)

	hold := envDuration(t, envCalHold, 10*time.Second)
	for _, concurrency := range []int{1, 4, 16, 64} {
		rate := measureInsertRate(t, writer, concurrency, hold)
		t.Logf("CDR write path: %2d concurrent writers -> %6.0f single-row Insert/s (%.0f ClickHouse statements/s)",
			concurrency, rate, 2*rate)
		if rate == 0 {
			t.Fatalf("the CDR writer moved nothing at concurrency %d", concurrency)
		}
	}
}

// measureInsertRate holds `concurrency` writers against the CDR path for the whole window and returns
// the rate in single-row inserts per second.
func measureInsertRate(t *testing.T, writer *clickhouse.CDRWriter, concurrency int, hold time.Duration) float64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), hold)
	defer cancel()

	var done atomic.Uint64
	var wg sync.WaitGroup
	start := time.Now()
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if err := writer.Insert(ctx, benchRow()); err != nil {
					// A cancelled write is the window closing, not a failure; anything else is worth
					// seeing, but the loop keeps going so one blip does not zero the measurement.
					if ctx.Err() == nil {
						t.Logf("insert: %v", err)
					}
					return
				}
				done.Add(1)
			}
		}()
	}
	wg.Wait()
	return float64(done.Load()) / time.Since(start).Seconds()
}

// benchRow is the row connectorpool writes on a submit_sm_resp: an enroute MT with a connector, one
// segment, distinct ids so nothing collapses under the ReplacingMergeTree.
func benchRow() clickhouse.CDRRow {
	connectorID := uuid.New()
	return clickhouse.CDRRow{
		MessageID:    uuid.New(),
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   refSenderID,
		DestAddr:     "+2250700000000",
		ConnectorID:  &connectorID,
		SubmittedAt:  time.Now().UTC(),
		Status:       clickhouse.StatusEnroute,
		SegmentCount: 1,
		SegmentSeq:   1,
		Encoding:     clickhouse.EncodingGSM7,
	}
}
