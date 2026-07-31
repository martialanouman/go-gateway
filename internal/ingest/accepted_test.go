package ingest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

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
