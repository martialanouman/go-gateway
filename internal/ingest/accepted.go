package ingest

import (
	"context"
	"log/slog"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// CDRWriter writes a batch of CDR rows. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	InsertBatch(ctx context.Context, rows []clickhouse.CDRRow) error
}

// BatchConsumer runs a batch handler over a Kafka topic, committing at-least-once. *kafka.Consumer satisfies
// it. Declared consumer-side.
type BatchConsumer interface {
	RunBatch(ctx context.Context, handle kafka.BatchHandler) error
}

// AcceptedConsumer projects the accepted CDR row from the durable mt.inbound topic (§1.10, step-101/162). The
// accepted row was previously written best-effort off the request path and dropped under saturation — a lost
// CDR. Here it is DERIVED from mt.inbound (the record that already earned the submission its acknowledgement)
// by a dedicated consumer group, committing the offset only after a successful ClickHouse write, so no
// accepted row is ever lost: a ClickHouse fault leaves the offset uncommitted and the batch is reprocessed.
//
// It runs on its OWN consumer group, independent of routing, so a slow ClickHouse only lengthens the
// get-message 404 window (the enroute row supersedes accepted) and never blocks the MT routing path.
//
// The body is sealed into the row per the customer's content policy (step-162). Sealing NEVER fails the
// record: an unavailable data key drops the body (counted) but still writes the row and commits — content is
// non-blocking; only the durable row write gates the commit (invariant a holds: the body reaches only the
// content column, never a log).
type AcceptedConsumer struct {
	consumer BatchConsumer
	cdr      CDRWriter
	sealer   *ContentSealer
	logger   *slog.Logger
	topic    string
	project  func(kafka.Record) (clickhouse.CDRRow, msg.Body, error)
}

// NewAcceptedConsumer wires the consumer. sealer may be nil: content storage is then disabled (no body is
// ever written), the row is still projected durably.
func NewAcceptedConsumer(consumer BatchConsumer, cdr CDRWriter, sealer *ContentSealer, logger *slog.Logger) *AcceptedConsumer {
	return newAcceptedConsumer(consumer, cdr, sealer, logger, "mt.inbound", projectInbound)
}

// NewMOCDRConsumer projects the CDR row of each routed MO off mo.routed, with the same durability as the
// MT accepted row. It is written there and not after delivery: a ClickHouse fault on the delivery path
// would replay the delivery, and hand the MO to the account twice.
func NewMOCDRConsumer(consumer BatchConsumer, cdr CDRWriter, sealer *ContentSealer, logger *slog.Logger) *AcceptedConsumer {
	return newAcceptedConsumer(consumer, cdr, sealer, logger, "mo.routed", projectMORouted)
}

func newAcceptedConsumer(consumer BatchConsumer, cdr CDRWriter, sealer *ContentSealer, logger *slog.Logger,
	topic string, project func(kafka.Record) (clickhouse.CDRRow, msg.Body, error)) *AcceptedConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &AcceptedConsumer{consumer: consumer, cdr: cdr, sealer: sealer, logger: logger, topic: topic, project: project}
}

func projectInbound(rec kafka.Record) (clickhouse.CDRRow, msg.Body, error) {
	env, err := pipeline.DecodeInbound(rec)
	if err != nil {
		return clickhouse.CDRRow{}, msg.Body{}, err
	}
	row := AcceptedRow(env)
	// The heavy phone parse the request path deferred: normalize to the canonical form the router stores,
	// so a message spells its destination the same across all its lifecycle rows. Best-effort — a number
	// that fails to parse keeps its raw form (the router is the single rejection authority).
	if norm, nerr := e164.Normalize(row.DestAddr); nerr == nil {
		row.DestAddr = norm
	}
	return row, env.Body, nil
}

// projectMORouted writes the MO as accepted: received and routed to its account. Its delivery to the
// account is not projected (debts/issue-de-remise-mo-non-projetee.md), and delivered would be a lie for
// an MO that ends on the dead-letter.
func projectMORouted(rec kafka.Record) (clickhouse.CDRRow, msg.Body, error) {
	mo, err := pipeline.DecodeMORouted(rec)
	if err != nil {
		return clickhouse.CDRRow{}, msg.Body{}, err
	}
	connectorID := mo.ConnectorID
	return clickhouse.CDRRow{
		MessageID:    mo.MessageID,
		TraceID:      mo.TraceID,
		AccountID:    mo.AccountID,
		CustomerID:   mo.CustomerID,
		Direction:    clickhouse.DirectionMO,
		SourceAddr:   mo.From,
		DestAddr:     mo.To,
		ConnectorID:  &connectorID,
		SubmittedAt:  mo.ReceivedAt,
		Status:       clickhouse.StatusAccepted,
		SegmentCount: mo.SegmentCount,
		SegmentSeq:   1,
		Encoding:     clickhouse.EncodingOf(mo.Encoding),
	}, mo.Body, nil
}

// Run consumes mt.inbound and writes accepted rows until ctx is cancelled. It is one supervised goroutine.
func (c *AcceptedConsumer) Run(ctx context.Context) error {
	return c.consumer.RunBatch(ctx, c.handleBatch)
}

// handleBatch builds an accepted row per record and writes the whole poll batch to ClickHouse in one
// InsertBatch. It is all-or-nothing per poll batch: on a ClickHouse fault every record's offset stays
// uncommitted so the batch reprocesses (redelivery is safe — the CDR is a ReplacingMergeTree keyed by its
// versioned rows at a fixed version, so a re-inserted accepted row dedups, and each row is self-coherent —
// ciphertext and content_key_id together — so the surviving tie decrypts. The only narrow edge is a DEK
// rotation between the first insert and a reprocess: the two ties then carry different key ids, and a read
// before the background merge could pair a ciphertext with the wrong key id — an undecryptable body, never a
// lost or wrong message). A record that cannot be decoded is a corrupt record,
// not a transient fault: it is logged and skipped (never written, but committable) so it cannot poison-pill
// the stream — though if the batch's write also fails, it reprocesses with the rest until ClickHouse recovers.
func (c *AcceptedConsumer) handleBatch(ctx context.Context, recs []kafka.Record) []error {
	results := make([]error, len(recs))
	rows := make([]clickhouse.CDRRow, 0, len(recs))
	for _, rec := range recs {
		row, body, err := c.project(rec)
		if err != nil {
			// A corrupt record can never become valid; skip it rather than stall the stream forever.
			c.logger.ErrorContext(ctx, "decode record for accepted CDR: skipping corrupt record", "topic", c.topic, "err", err)
			continue
		}
		if c.sealer != nil {
			c.sealer.Seal(ctx, &row, body, row.CustomerID)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return results // nothing decodable in this batch; all offsets committable
	}
	if err := c.cdr.InsertBatch(ctx, rows); err != nil {
		if ctx.Err() != nil {
			return results // graceful shutdown mid-batch: let the supervisor stop cleanly
		}
		c.logger.ErrorContext(ctx, "accepted-row batch write failed; will reprocess", "rows", len(rows), "err", err)
		for i := range results {
			results[i] = err // fail the whole poll batch → nothing commits → reprocess
		}
	}
	return results
}
