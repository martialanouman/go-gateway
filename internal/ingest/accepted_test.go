package ingest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// capturingConsumer runs the handler once over a fixed set of records and captures the per-record results.
type capturingConsumer struct {
	recs    []kafka.Record
	results []error
}

func (c *capturingConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	c.results = handle(ctx, c.recs)
	return nil
}

// errCDR fails every InsertBatch — the transient ClickHouse fault the durable consumer must not lose data on.
type errCDR struct{ err error }

func (e errCDR) InsertBatch(context.Context, []clickhouse.CDRRow) error { return e.err }

func inboundRec(t *testing.T, cust uuid.UUID) kafka.Record {
	t.Helper()
	rec, err := pipeline.EncodeInbound(pipeline.InboundMT{
		MessageID: uuid.New(), CustomerID: cust, AccountID: uuid.New(),
		From: "SENDER", To: "33612345678", Encoding: "gsm7", Body: msg.NewBodyString("hi"),
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return rec
}

// TestAcceptedConsumerWritesBatchAndCommits: a batch of valid records is written to ClickHouse and every
// record reports handled (nil), so their offsets commit.
func TestAcceptedConsumerWritesBatchAndCommits(t *testing.T) {
	cdr := &fakeCDR{}
	cons := &capturingConsumer{recs: []kafka.Record{inboundRec(t, uuid.New()), inboundRec(t, uuid.New())}}
	ac := ingest.NewAcceptedConsumer(cons, cdr, nil, nil)

	if err := ac.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 2 {
		t.Fatalf("wrote %d rows, want 2", len(cdr.rows))
	}
	for i, r := range cons.results {
		if r != nil {
			t.Errorf("record %d result = %v, want nil (committable)", i, r)
		}
	}
}

// TestAcceptedConsumerReprocessesOnWriteFailure: a ClickHouse fault fails EVERY record in the poll batch, so
// nothing commits and the batch is redelivered — no accepted row is lost (the point of step-101).
func TestAcceptedConsumerReprocessesOnWriteFailure(t *testing.T) {
	cons := &capturingConsumer{recs: []kafka.Record{inboundRec(t, uuid.New()), inboundRec(t, uuid.New())}}
	ac := ingest.NewAcceptedConsumer(cons, errCDR{err: errors.New("clickhouse down")}, nil, nil)

	if err := ac.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, r := range cons.results {
		if r == nil {
			t.Errorf("record %d committed despite the write failure — a CDR would be lost", i)
		}
	}
}

// TestAcceptedConsumerSkipsCorruptRecord: a record that cannot be decoded is skipped (committable), never
// poison-pilling the stream; the valid record in the same batch is still written.
func TestAcceptedConsumerSkipsCorruptRecord(t *testing.T) {
	cdr := &fakeCDR{}
	corrupt := kafka.Record{Value: []byte("not json")}
	cons := &capturingConsumer{recs: []kafka.Record{corrupt, inboundRec(t, uuid.New())}}
	ac := ingest.NewAcceptedConsumer(cons, cdr, nil, nil)

	if err := ac.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 1 {
		t.Fatalf("wrote %d rows, want 1 (corrupt skipped)", len(cdr.rows))
	}
	for i, r := range cons.results {
		if r != nil {
			t.Errorf("record %d result = %v, want nil", i, r)
		}
	}
}

// TestMOCDRConsumerProjectsTheRoutedMO: one accepted row per routed MO, attributed to the connector that
// received it and the customer it was routed to, its body sealed by the customer's content policy.
func TestMOCDRConsumerProjectsTheRoutedMO(t *testing.T) {
	mo := pipeline.MORouted{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		ConnectorID: uuid.New(), From: "+22507000001", To: "36000", Body: msg.NewBodyString("hello"),
		Encoding: "ucs2", ReceivedAt: time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC), SegmentCount: 3,
	}
	rec, err := pipeline.EncodeMORouted(mo)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	cdr := &fakeCDR{}
	sealer := ingest.NewContentSealer(policyOf{mo.CustomerID: cp.ContentStoredPlaintext}, fakeKeys{}, nil, nil)
	cons := &capturingConsumer{recs: []kafka.Record{{Value: []byte("not json")}, rec}}

	if err := ingest.NewMOCDRConsumer(cons, cdr, sealer, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows) != 1 {
		t.Fatalf("wrote %d rows, want 1 (the corrupt record skipped)", len(cdr.rows))
	}
	got := cdr.rows[0]
	if got.MessageID != mo.MessageID || got.TraceID != mo.TraceID || got.AccountID != mo.AccountID ||
		got.CustomerID != mo.CustomerID || got.ConnectorID == nil || *got.ConnectorID != mo.ConnectorID ||
		got.Direction != clickhouse.DirectionMO || got.Status != clickhouse.StatusAccepted ||
		got.SourceAddr != mo.From || got.DestAddr != mo.To || !got.SubmittedAt.Equal(mo.ReceivedAt) ||
		got.SegmentCount != 3 || got.SegmentSeq != 1 || got.Encoding != clickhouse.EncodingUCS2 {
		t.Errorf("row = %+v, want the routed MO projected as accepted", got)
	}
	if got.ContentCiphertext == nil || *got.ContentCiphertext != "hello" {
		t.Errorf("content = %v, want the body stored per the plaintext policy", got.ContentCiphertext)
	}
	for i, r := range cons.results {
		if r != nil {
			t.Errorf("record %d result = %v, want nil (committable)", i, r)
		}
	}
}

// TestAcceptedConsumerKeepsTheBatchWhenStoppedMidWrite: a write cut short by shutdown may never have reached
// ClickHouse, so the batch stays uncommitted and is read again on restart, where the rows dedup.
func TestAcceptedConsumerKeepsTheBatchWhenStoppedMidWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cons := &capturingConsumer{recs: []kafka.Record{inboundRec(t, uuid.New())}}
	if err := ingest.NewAcceptedConsumer(cons, errCDR{err: context.Canceled}, nil, nil).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cons.results[0] == nil {
		t.Error("the record committed although its write was cancelled: its row may be lost")
	}
}

// policyOf stores content for the customers it names only, so a body sealed under the wrong customer reads off.
type policyOf map[uuid.UUID]cp.ContentStorage

func (p policyOf) For(id uuid.UUID) cp.ContentStorage {
	if s, ok := p[id]; ok {
		return s
	}
	return cp.ContentOff
}
