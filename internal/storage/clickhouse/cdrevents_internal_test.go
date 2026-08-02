package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// errTimeline is what the fake connection fails the cdr_events write with.
var errTimeline = errors.New("timeline table unavailable")

// eventsFailingConn serves the cdr batch normally and fails only the cdr_events one. Embedding
// driver.Conn rather than implementing it keeps the fake to the one method under test: anything else
// the writer might call would panic loudly instead of being silently satisfied.
type eventsFailingConn struct {
	driver.Conn
	cdrPrepared bool
}

func (c *eventsFailingConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if strings.Contains(query, "cdr_events") {
		return nil, errTimeline
	}
	c.cdrPrepared = true
	return &noopBatch{}, nil
}

// noopBatch accepts every row and reports a successful send: the cdr leg must look healthy, so that a
// failure surfacing from InsertBatch can only have come from the timeline.
type noopBatch struct {
	driver.Batch
	rows int
	sent bool
}

func (b *noopBatch) Append(...any) error { b.rows++; return nil }
func (b *noopBatch) Send() error         { b.sent = true; return nil }
func (b *noopBatch) Abort() error        { return nil }

// A timeline write that fails must not fail the CDR write. The two tables have no atomicity between
// them: when appendEvents fails, the cdr rows are ALREADY durable, so propagating the error makes the
// caller redeliver work that succeeded. On the connector pool that redelivery re-submits a message
// already sent to the SMSC — a duplicate SMS, to save a dashboard timeline.
//
// The cdr table is the billing and reporting authority; cdr_events is dashboard comfort. The header
// comment on appendEvents already said so; the code did the opposite.
func TestTimelineFailureDoesNotFailTheCDR(t *testing.T) {
	conn := &eventsFailingConn{}
	w := &CDRWriter{conn: conn}

	row := CDRRow{
		MessageID: uuid.New(), TraceID: uuid.New(),
		AccountID: uuid.New(), CustomerID: uuid.New(),
		Direction: DirectionMT, SourceAddr: "GATEWAY", DestAddr: "33612345678",
		SubmittedAt: time.Now().UTC(), Encoding: EncodingGSM7, SegmentCount: 1,
		Status: StatusEnroute, SegmentSeq: 1,
	}

	if err := w.InsertBatch(context.Background(), []CDRRow{row}); err != nil {
		t.Errorf("InsertBatch = %v, want nil: the cdr rows are durable, only the timeline failed", err)
	}
	// Fixture guard: if the cdr leg had not run, this test would prove nothing about which leg failed.
	if !conn.cdrPrepared {
		t.Fatal("fixture: the cdr batch was never prepared, so the failure did not come from the timeline")
	}
}

// The failure must still be observable. Swallowing it silently would trade one dead guard for another:
// a timeline that stops being written would look exactly like a timeline nobody queries.
func TestTimelineFailureIsReported(t *testing.T) {
	var got error
	w := &CDRWriter{conn: &eventsFailingConn{}, onTimelineError: func(err error) { got = err }}

	row := CDRRow{
		MessageID: uuid.New(), TraceID: uuid.New(),
		AccountID: uuid.New(), CustomerID: uuid.New(),
		Direction: DirectionMT, SourceAddr: "GATEWAY", DestAddr: "33612345678",
		SubmittedAt: time.Now().UTC(), Encoding: EncodingGSM7, SegmentCount: 1,
		Status: StatusEnroute, SegmentSeq: 1,
	}
	if err := w.InsertBatch(context.Background(), []CDRRow{row}); err != nil {
		t.Fatalf("InsertBatch = %v, want nil", err)
	}

	if got == nil {
		t.Fatal("the timeline failure was swallowed without being reported")
	}
	if !errors.Is(got, errTimeline) {
		t.Errorf("reported error = %v, want it to wrap %v", got, errTimeline)
	}
}
