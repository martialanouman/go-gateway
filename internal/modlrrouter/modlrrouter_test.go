package modlrrouter_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

type fakeConsumer struct{ records []kafka.Record }

func (f *fakeConsumer) Run(ctx context.Context, handle kafka.Handler) error {
	for _, r := range f.records {
		if err := handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

type fakeResolver struct {
	m     dlrmap.Mapping
	found bool
	err   error
}

func (f fakeResolver) Get(context.Context, uuid.UUID, string) (dlrmap.Mapping, bool, error) {
	return f.m, f.found, f.err
}

type fakeCDR struct {
	mu   sync.Mutex
	rows []clickhouse.CDRRow
}

func (f *fakeCDR) Insert(_ context.Context, row clickhouse.CDRRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, row)
	return nil
}

type fakeCounter struct{ n int }

func (c *fakeCounter) Inc() { c.n++ }

// sampleMapping is a resolved correlation with a submit time 5s before delivery.
func sampleMapping() dlrmap.Mapping {
	return dlrmap.Mapping{
		MessageID:    uuid.New(),
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		SourceAddr:   "GATEWAY",
		DestAddr:     "+22507000000",
		ConnectorID:  uuid.New(),
		SegmentCount: 1,
		Encoding:     "gsm7",
		SubmittedAt:  time.Now().UTC().Add(-5 * time.Second),
	}
}

// dlrRecord encodes a dlr.events record for the given connector/smsc id/state.
func dlrRecord(t *testing.T, connectorID uuid.UUID, smscID string, state uint8) kafka.Record {
	t.Helper()
	rec, err := pipeline.EncodeDLR(pipeline.DLREvent{
		ConnectorID:   connectorID,
		SMSCMessageID: smscID,
		State:         state,
		ReceivedAt:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("encode dlr: %v", err)
	}
	return rec
}

func runOne(t *testing.T, deps modlrrouter.Deps, rec kafka.Record) error {
	t.Helper()
	deps.Consumer = &fakeConsumer{records: []kafka.Record{rec}}
	if deps.Tracer == nil {
		deps.Tracer = observability.Tracer(nil, "test")
	}
	return modlrrouter.New(deps).Run(context.Background())
}

// TestRouterWritesDeliveredCDR: a delivered receipt correlated to a mapping produces a delivered CDR
// row carrying the message projection, delivered_at, a computed latency and no error code.
func TestRouterWritesDeliveredCDR(t *testing.T) {
	m := sampleMapping()
	cdr := &fakeCDR{}
	err := runOne(t, modlrrouter.Deps{
		Resolver: fakeResolver{m: m, found: true},
		CDR:      cdr,
	}, dlrRecord(t, m.ConnectorID, "smsc-1", smpp.MessageStateDelivered))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 1 {
		t.Fatalf("want 1 CDR row, got %d", len(cdr.rows))
	}
	row := cdr.rows[0]
	if row.Status != clickhouse.StatusDelivered {
		t.Errorf("status = %q, want delivered", row.Status)
	}
	if row.ErrorCode != nil {
		t.Errorf("error_code = %v, want nil for a delivery", *row.ErrorCode)
	}
	if row.DeliveredAt == nil {
		t.Error("delivered_at must be set")
	}
	if row.LatencyMs == nil || *row.LatencyMs < 4000 || *row.LatencyMs > 6000 {
		t.Errorf("latency_ms = %v, want ~5000", row.LatencyMs)
	}
	// The projection columns come from the mapping so the delivered row does not blank them.
	if row.MessageID != m.MessageID || row.AccountID != m.AccountID || row.CustomerID != m.CustomerID ||
		row.SourceAddr != m.SourceAddr || row.DestAddr != m.DestAddr || row.Direction != clickhouse.DirectionMT {
		t.Errorf("row projection = %+v, want mapping %+v", row, m)
	}
}

// TestRouterWritesFailedWithCode: an undeliverable receipt yields a failed row with delivery_failed.
func TestRouterWritesFailedWithCode(t *testing.T) {
	m := sampleMapping()
	cdr := &fakeCDR{}
	if err := runOne(t, modlrrouter.Deps{Resolver: fakeResolver{m: m, found: true}, CDR: cdr},
		dlrRecord(t, m.ConnectorID, "smsc-2", smpp.MessageStateUndeliverable)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	row := cdr.rows[0]
	if row.Status != clickhouse.StatusFailed || row.ErrorCode == nil || *row.ErrorCode != "delivery_failed" {
		t.Errorf("row = status %q / code %v, want failed / delivery_failed", row.Status, row.ErrorCode)
	}
	// A failed message was never delivered: no delivered_at, no latency.
	if row.DeliveredAt != nil || row.LatencyMs != nil {
		t.Errorf("failed row must not set delivered_at/latency_ms: delivered_at=%v latency=%v", row.DeliveredAt, row.LatencyMs)
	}
}

// TestRouterWritesExpiredWithCode: an expired receipt yields an expired row with delivery_expired.
func TestRouterWritesExpiredWithCode(t *testing.T) {
	m := sampleMapping()
	cdr := &fakeCDR{}
	if err := runOne(t, modlrrouter.Deps{Resolver: fakeResolver{m: m, found: true}, CDR: cdr},
		dlrRecord(t, m.ConnectorID, "smsc-3", smpp.MessageStateExpired)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	row := cdr.rows[0]
	if row.Status != clickhouse.StatusExpired || row.ErrorCode == nil || *row.ErrorCode != "delivery_expired" {
		t.Errorf("row = status %q / code %v, want expired / delivery_expired", row.Status, row.ErrorCode)
	}
	if row.DeliveredAt != nil || row.LatencyMs != nil {
		t.Errorf("expired row must not set delivered_at/latency_ms: delivered_at=%v latency=%v", row.DeliveredAt, row.LatencyMs)
	}
}

// TestRouterSkipsNonTerminalState: an intermediate receipt (enroute) records nothing.
func TestRouterSkipsNonTerminalState(t *testing.T) {
	m := sampleMapping()
	cdr := &fakeCDR{}
	counter := &fakeCounter{}
	if err := runOne(t, modlrrouter.Deps{Resolver: fakeResolver{m: m, found: true}, CDR: cdr, Unmapped: counter},
		dlrRecord(t, m.ConnectorID, "smsc-4", smpp.MessageStateEnroute)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 0 || counter.n != 0 {
		t.Errorf("non-terminal receipt should write nothing: rows=%d count=%d", len(cdr.rows), counter.n)
	}
}

// TestRouterCountsUnmappedDLR: a terminal receipt with no mapping is counted and committed, not
// dropped silently and not written.
func TestRouterCountsUnmappedDLR(t *testing.T) {
	cdr := &fakeCDR{}
	counter := &fakeCounter{}
	err := runOne(t, modlrrouter.Deps{Resolver: fakeResolver{found: false}, CDR: cdr, Unmapped: counter},
		dlrRecord(t, uuid.New(), "smsc-missing", smpp.MessageStateDelivered))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if counter.n != 1 {
		t.Errorf("unmapped counter = %d, want 1", counter.n)
	}
	if len(cdr.rows) != 0 {
		t.Errorf("an unmapped receipt must write no CDR row, got %d", len(cdr.rows))
	}
}

// TestRouterRetriesOnResolverError: a Redis infra error is transient — the handler returns an error so
// the offset is not committed and the receipt is reprocessed.
func TestRouterRetriesOnResolverError(t *testing.T) {
	cdr := &fakeCDR{}
	err := runOne(t, modlrrouter.Deps{Resolver: fakeResolver{err: errors.New("redis down")}, CDR: cdr},
		dlrRecord(t, uuid.New(), "smsc-5", smpp.MessageStateDelivered))
	if err == nil {
		t.Error("a resolver error must fail the handler (reprocess), got nil")
	}
	if len(cdr.rows) != 0 {
		t.Errorf("no CDR row on a resolver error, got %d", len(cdr.rows))
	}
}

// TestRouterSkipsUndecodableRecord: a garbage record is logged and committed, never wedging the
// partition.
func TestRouterSkipsUndecodableRecord(t *testing.T) {
	cdr := &fakeCDR{}
	counter := &fakeCounter{}
	err := runOne(t, modlrrouter.Deps{Resolver: fakeResolver{found: false}, CDR: cdr, Unmapped: counter},
		kafka.Record{Topic: kafka.TopicDLREvents, Value: []byte("not json")})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 0 || counter.n != 0 {
		t.Errorf("undecodable record should do nothing: rows=%d count=%d", len(cdr.rows), counter.n)
	}
}
