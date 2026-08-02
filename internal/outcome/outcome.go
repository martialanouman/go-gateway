// Package outcome projects the terminal outcome of a submitted MT segment — enroute or failed —
// from the mt.outcome topic onto the CDR. It is the read side of the split step-201c made: the
// connector pool publishes the outcome and commits, this consumer writes the row.
//
// It sits beside internal/ingest (the accepted-row projection off mt.inbound) rather than inside it:
// same shape, different topic, different producer and its own consumer group, so neither projection
// can stall the other.
package outcome

import (
	"context"
	"log/slog"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// CDRWriter writes a batch of CDR rows. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	InsertBatch(ctx context.Context, rows []clickhouse.CDRRow) error
}

// BatchConsumer runs a batch handler over a Kafka topic, committing at-least-once. *kafka.Consumer
// satisfies it. Declared consumer-side.
type BatchConsumer interface {
	RunBatch(ctx context.Context, handle kafka.BatchHandler) error
}

// Projector consumes mt.outcome and writes the enroute/failed CDR row (step-201c, D1).
//
// The connector pool used to write that row itself, synchronously, before committing the offset of the
// message it had just put on the SMSC wire — four ClickHouse round trips per message on the consumption
// path, which capped the pool far below its SMSC throughput. Batching it there was not an option: a
// failed write would have redelivered the whole poll and RE-SUBMITTED messages already sent. Behind a
// topic, the same batching is harmless — a redelivery here rewrites a row and nothing else, because the
// submit_sm is long gone and already committed, and `cdr` is a ReplacingMergeTree keyed by the row's
// identity and versioned by the status rank, so a replayed outcome collapses onto itself.
//
// It runs on its OWN consumer group, so a slow ClickHouse only lengthens the window in which
// get-message still reads the previous status; it never touches the send path.
type Projector struct {
	consumer BatchConsumer
	cdr      CDRWriter
	logger   *slog.Logger
}

// NewProjector wires the projection. A nil logger falls back to slog.Default().
func NewProjector(consumer BatchConsumer, cdr CDRWriter, logger *slog.Logger) *Projector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Projector{consumer: consumer, cdr: cdr, logger: logger}
}

// Run consumes mt.outcome and writes outcome rows until ctx is cancelled. It is one supervised
// goroutine.
func (p *Projector) Run(ctx context.Context) error {
	return p.consumer.RunBatch(ctx, p.handleBatch)
}

// handleBatch builds a row per record and writes the whole poll batch to ClickHouse in one InsertBatch.
// It is all-or-nothing per poll batch: on a ClickHouse fault EVERY record's offset stays uncommitted —
// including the ones skipped as corrupt, which have nothing to write but whose commit would advance the
// offset past their unwritten neighbours. A record that cannot be decoded, or that carries a status this
// build does not know, is a corrupt record rather than a transient fault: it is logged and skipped so it
// cannot poison-pill the stream.
func (p *Projector) handleBatch(ctx context.Context, recs []kafka.Record) []error {
	results := make([]error, len(recs))
	rows := make([]clickhouse.CDRRow, 0, len(recs))
	for _, rec := range recs {
		env, err := pipeline.DecodeOutcome(rec)
		if err != nil {
			// A corrupt record can never become valid; skip it rather than stall the stream forever.
			p.logger.ErrorContext(ctx, "decode mt.outcome for CDR projection: skipping corrupt record", "err", err)
			continue
		}
		status := clickhouse.Status(env.Status)
		if !status.Valid() {
			// An unknown status ranks 0, so its row would supersede nothing — it would sit under the accepted
			// row forever, leaving the message frozen at "accepted" with no trace of why. Writing it is worse
			// than skipping it, and it is no more likely to become valid on a retry than a bad decode is.
			p.logger.ErrorContext(ctx, "unknown status on mt.outcome: skipping corrupt record",
				"status", env.Status, "message_id", env.MessageID)
			continue
		}
		rows = append(rows, row(env, status))
	}
	if len(rows) == 0 {
		return results // nothing writable in this batch; all offsets committable
	}
	if err := p.cdr.InsertBatch(ctx, rows); err != nil {
		if ctx.Err() != nil {
			return results // graceful shutdown mid-batch: let the supervisor stop cleanly
		}
		p.logger.ErrorContext(ctx, "outcome-row batch write failed; will reprocess", "rows", len(rows), "err", err)
		for i := range results {
			results[i] = err // fail the whole poll batch → nothing commits → reprocess
		}
	}
	return results
}

// row projects one outcome onto its CDR row — the exact row the connector pool wrote at the submit site
// before step-201c, field for field.
//
// The nil fields are nil on purpose, not by omission: original_source_addr and routing_script_id belong
// to the router's rows, delivered_at and latency_ms to the DLR path, and the content columns to the
// accepted row alone (the outcome carries no body — invariant a). `version` is left unset: the writer
// derives it from Status.
//
// The segment coordinates are copied verbatim. The producer already clamps them to >= 1 (it is the only
// party that knows a connector row is always a dispatched segment), and segment_seq joins the CDR
// sorting key, so re-deriving it here could only move the row off the one it must supersede.
func row(env pipeline.OutcomeMT, status clickhouse.Status) clickhouse.CDRRow {
	connectorID := env.ConnectorID
	return clickhouse.CDRRow{
		MessageID:   env.MessageID,
		TraceID:     env.TraceID,
		AccountID:   env.AccountID,
		CustomerID:  env.CustomerID,
		Direction:   clickhouse.DirectionMT,
		SourceAddr:  env.From,
		DestAddr:    env.To,
		ConnectorID: &connectorID,
		RouteID:     env.RouteID,
		SubmittedAt: env.SubmittedAt,
		Status:      status,
		ErrorCode:   env.ErrorCode,
		//nolint:gosec // segment coordinates are small positive integers, clamped by the producer; this
		// reproduces the pre-step-201c conversion exactly.
		SegmentCount: uint16(env.SegmentCount),
		//nolint:gosec // idem.
		SegmentSeq:     uint16(env.SegmentSeq),
		Encoding:       clickhouse.EncodingOf(env.Encoding),
		Billed:         env.Billed,
		CreditsCharged: env.CreditsCharged,
	}
}
