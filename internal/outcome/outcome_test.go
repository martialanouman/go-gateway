package outcome_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/outcome"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// capturingConsumer runs the handler once over a fixed set of records and captures the per-record
// results — the offsets the real consumer would commit.
type capturingConsumer struct {
	recs    []kafka.Record
	results []error
}

func (c *capturingConsumer) RunBatch(ctx context.Context, handle kafka.BatchHandler) error {
	c.results = handle(ctx, c.recs)
	return nil
}

// fakeCDR records every InsertBatch call, so a test can assert BOTH the rows and that the whole poll
// batch went out in a single write.
type fakeCDR struct {
	batches [][]clickhouse.CDRRow
}

func (f *fakeCDR) InsertBatch(_ context.Context, rows []clickhouse.CDRRow) error {
	f.batches = append(f.batches, append([]clickhouse.CDRRow(nil), rows...))
	return nil
}

func (f *fakeCDR) rows() []clickhouse.CDRRow {
	var all []clickhouse.CDRRow
	for _, b := range f.batches {
		all = append(all, b...)
	}
	return all
}

// errCDR fails every InsertBatch — the transient ClickHouse fault the projection must not lose a row on.
type errCDR struct{ err error }

func (e errCDR) InsertBatch(context.Context, []clickhouse.CDRRow) error { return e.err }

// cancellingCDR cancels the run's context and then fails, reproducing a write aborted by a shutdown.
type cancellingCDR struct{ cancel context.CancelFunc }

func (c cancellingCDR) InsertBatch(context.Context, []clickhouse.CDRRow) error {
	c.cancel()
	return errors.New("context canceled")
}

var submittedAt = time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)

// outcomeRec encodes an mt.outcome record the connector pool would publish.
func outcomeRec(t *testing.T, env pipeline.OutcomeMT) kafka.Record {
	t.Helper()
	rec, err := pipeline.EncodeOutcome(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return rec
}

// enrouteEvent is a fully-populated successful outcome: every field the connector fills.
func enrouteEvent() pipeline.OutcomeMT {
	routeID := uuid.New()
	return pipeline.OutcomeMT{
		MessageID:    uuid.New(),
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		ConnectorID:  uuid.New(),
		RouteID:      &routeID,
		From:         "ACME",
		To:           "+2250700000000",
		Encoding:     "ucs2",
		SegmentSeq:   2,
		SegmentCount: 3,
		SubmittedAt:  submittedAt,
		Status:       string(clickhouse.StatusEnroute),
	}
}

// TestProjectorWritesTheRowTheConnectorUsedToWrite pins the projection field by field against what
// cdrRow produced at the submit site before step-201c. The whole-struct comparison is deliberate: a
// field silently filled (or silently dropped) changes the CDR without changing any test that only
// looked at the fields it cared about.
func TestProjectorWritesTheRowTheConnectorUsedToWrite(t *testing.T) {
	event := enrouteEvent()
	cdr := &fakeCDR{}
	cons := &capturingConsumer{recs: []kafka.Record{outcomeRec(t, event)}}

	if err := outcome.NewProjector(cons, cdr, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rows := cdr.rows()
	if len(rows) != 1 {
		t.Fatalf("wrote %d rows, want 1", len(rows))
	}
	connectorID := event.ConnectorID
	want := clickhouse.CDRRow{
		MessageID:    event.MessageID,
		TraceID:      event.TraceID,
		AccountID:    event.AccountID,
		CustomerID:   event.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   event.From,
		DestAddr:     event.To,
		ConnectorID:  &connectorID,
		RouteID:      event.RouteID,
		SubmittedAt:  event.SubmittedAt,
		Status:       clickhouse.StatusEnroute,
		SegmentCount: 3,
		SegmentSeq:   2,
		Encoding:     clickhouse.EncodingUCS2,
	}
	if !reflect.DeepEqual(rows[0], want) {
		t.Errorf("row = %+v, want %+v", rows[0], want)
	}
}

// TestProjectorCarriesTheFailedOutcome: a permanent SMSC rejection projects its gateway error code and
// its settlement, unchanged.
func TestProjectorCarriesTheFailedOutcome(t *testing.T) {
	code := "smsc_rejected"
	charged := int32(4)
	event := enrouteEvent()
	event.Status = string(clickhouse.StatusFailed)
	event.ErrorCode = &code
	event.Billed = true
	event.CreditsCharged = &charged

	cdr := &fakeCDR{}
	cons := &capturingConsumer{recs: []kafka.Record{outcomeRec(t, event)}}
	if err := outcome.NewProjector(cons, cdr, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rows := cdr.rows()
	if len(rows) != 1 {
		t.Fatalf("wrote %d rows, want 1", len(rows))
	}
	got := rows[0]
	if got.Status != clickhouse.StatusFailed {
		t.Errorf("Status = %v, want %v", got.Status, clickhouse.StatusFailed)
	}
	if got.ErrorCode == nil || *got.ErrorCode != code {
		t.Errorf("ErrorCode = %v, want %v", got.ErrorCode, code)
	}
	if !got.Billed {
		t.Errorf("Billed = %v, want true", got.Billed)
	}
	if got.CreditsCharged == nil || *got.CreditsCharged != charged {
		t.Errorf("CreditsCharged = %v, want %v", got.CreditsCharged, charged)
	}
}

// TestProjectorWritesThePollBatchInOneInsert: the whole poll batch goes out in a SINGLE InsertBatch —
// the reason the row moved off the send path at all — and every record reports committable.
func TestProjectorWritesThePollBatchInOneInsert(t *testing.T) {
	cdr := &fakeCDR{}
	cons := &capturingConsumer{recs: []kafka.Record{
		outcomeRec(t, enrouteEvent()), outcomeRec(t, enrouteEvent()), outcomeRec(t, enrouteEvent()),
	}}

	if err := outcome.NewProjector(cons, cdr, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.batches) != 1 {
		t.Fatalf("InsertBatch called %d times, want 1 for the whole poll batch", len(cdr.batches))
	}
	if len(cdr.batches[0]) != 3 {
		t.Fatalf("batch carried %d rows, want 3", len(cdr.batches[0]))
	}
	for i, r := range cons.results {
		if r != nil {
			t.Errorf("record %d result = %v, want nil (committable)", i, r)
		}
	}
}

// TestProjectorSkipsCorruptRecord: a record that cannot be decoded can never become valid, so it is
// skipped (and committable) rather than stalling the stream forever; its batch-mates are still written.
func TestProjectorSkipsCorruptRecord(t *testing.T) {
	cdr := &fakeCDR{}
	cons := &capturingConsumer{recs: []kafka.Record{
		{Value: []byte("not json")}, outcomeRec(t, enrouteEvent()),
	}}

	if err := outcome.NewProjector(cons, cdr, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.rows()) != 1 {
		t.Fatalf("wrote %d rows, want 1 (corrupt skipped)", len(cdr.rows()))
	}
	for i, r := range cons.results {
		if r != nil {
			t.Errorf("record %d result = %v, want nil", i, r)
		}
	}
}

// TestProjectorSkipsAnUnknownStatus: an unknown status ranks 0 under the ReplacingMergeTree, so its row
// would supersede nothing and would sit under the accepted row forever — a message frozen at "accepted"
// with no trace of why. It is a corrupt record, not a write.
func TestProjectorSkipsAnUnknownStatus(t *testing.T) {
	bogus := enrouteEvent()
	bogus.Status = "handed-to-the-smsc"
	cdr := &fakeCDR{}
	cons := &capturingConsumer{recs: []kafka.Record{outcomeRec(t, bogus), outcomeRec(t, enrouteEvent())}}

	if err := outcome.NewProjector(cons, cdr, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rows := cdr.rows()
	if len(rows) != 1 {
		t.Fatalf("wrote %d rows, want 1 (unknown status skipped)", len(rows))
	}
	if rows[0].Status != clickhouse.StatusEnroute {
		t.Errorf("wrote the wrong row: Status = %v, want %v", rows[0].Status, clickhouse.StatusEnroute)
	}
	for i, r := range cons.results {
		if r != nil {
			t.Errorf("record %d result = %v, want nil", i, r)
		}
	}
}

// TestProjectorCopiesSegmentCoordinatesVerbatim: the producer already clamps segment_seq/segment_count
// to >= 1, so the projection must copy them unchanged. Re-clamping here would silently rewrite a
// coordinate that joins the CDR sorting key, moving the row off the one it is meant to supersede.
func TestProjectorCopiesSegmentCoordinatesVerbatim(t *testing.T) {
	event := enrouteEvent()
	event.SegmentSeq, event.SegmentCount = 0, 0

	cdr := &fakeCDR{}
	cons := &capturingConsumer{recs: []kafka.Record{outcomeRec(t, event)}}
	if err := outcome.NewProjector(cons, cdr, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rows := cdr.rows()
	if len(rows) != 1 {
		t.Fatalf("wrote %d rows, want 1", len(rows))
	}
	if rows[0].SegmentSeq != 0 || rows[0].SegmentCount != 0 {
		t.Errorf("SegmentSeq/SegmentCount = %d/%d, want 0/0 (copied verbatim, not re-clamped)",
			rows[0].SegmentSeq, rows[0].SegmentCount)
	}
}

// TestProjectorFailsTheWholeBatchOnAWriteFailure: a ClickHouse fault fails EVERY record of the poll
// batch — including the corrupt one that was never going to be written — so nothing commits and the
// whole poll is replayed. Committing the skipped record would advance the offset past its unwritten
// neighbours whenever it sits last in a partition.
func TestProjectorFailsTheWholeBatchOnAWriteFailure(t *testing.T) {
	cons := &capturingConsumer{recs: []kafka.Record{
		outcomeRec(t, enrouteEvent()), {Value: []byte("not json")}, outcomeRec(t, enrouteEvent()),
	}}

	p := outcome.NewProjector(cons, errCDR{err: errors.New("clickhouse down")}, nil)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, r := range cons.results {
		if r == nil {
			t.Errorf("record %d committed despite the write failure — a CDR row would be lost", i)
		}
	}
}

// TestProjectorCommitsABatchWithNothingToWrite: a poll carrying only undecodable records writes nothing
// at all — no empty InsertBatch — and commits, so the stream moves past them.
func TestProjectorCommitsABatchWithNothingToWrite(t *testing.T) {
	cdr := &fakeCDR{}
	cons := &capturingConsumer{recs: []kafka.Record{{Value: []byte("{")}, {Value: []byte("nope")}}}

	if err := outcome.NewProjector(cons, cdr, nil).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(cdr.batches) != 0 {
		t.Errorf("InsertBatch called %d times, want 0 (nothing decodable)", len(cdr.batches))
	}
	for i, r := range cons.results {
		if r != nil {
			t.Errorf("record %d result = %v, want nil (committable)", i, r)
		}
	}
}

// TestProjectorTreatsACancelledContextAsAGracefulStop: a write that fails because the process is
// shutting down is not a fault — reporting it would make the supervisor log a crash on every SIGTERM.
// The offsets stay committable; the rows are rewritten idempotently on the next start if they were lost.
func TestProjectorTreatsACancelledContextAsAGracefulStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cons := &capturingConsumer{recs: []kafka.Record{outcomeRec(t, enrouteEvent())}}

	if err := outcome.NewProjector(cons, cancellingCDR{cancel: cancel}, nil).Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, r := range cons.results {
		if r != nil {
			t.Errorf("record %d result = %v, want nil (graceful stop, not a fault)", i, r)
		}
	}
}
