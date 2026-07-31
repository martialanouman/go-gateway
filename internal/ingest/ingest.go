// Package ingest is the one MT ingestion path shared by every submission surface. A REST submit and
// an SMPP submit_sm both build a pipeline.InboundMT and hand it to Ingestor.Accept, which encodes it,
// writes it durably to mt.inbound (the durability boundary that earns the acknowledgement). The accepted
// CDR row is projected off that durable topic by AcceptedConsumer, not written on the caller's path. Both
// protocols reaching the pipeline through this single helper is what makes REST/SMPP parity a property of
// the code, not a coincidence.
//
// It never logs or spans a message body: the plaintext stays inside pipeline.InboundMT.Body until the
// audited WIRE codec reveals it into the durable Kafka value (invariant a).
package ingest

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// Producer publishes to mt.inbound. *kafka.Producer satisfies it. The produce is the durability
// boundary that earns a submission its acknowledgement (a REST 202, an SMPP submit_sm_resp).
type Producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// Ingestor performs the shared ingestion sequence for one submission: it encodes the envelope and produces
// it durably to mt.inbound (the boundary that earns the acknowledgement). The accepted CDR row is NOT written
// here — it is projected off the durable mt.inbound topic by AcceptedConsumer (step-101), so it can never be
// lost on the request path.
type Ingestor struct {
	producer Producer
	logger   *slog.Logger
}

// NewIngestor wires an Ingestor over the durable producer.
func NewIngestor(producer Producer, logger *slog.Logger) *Ingestor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Ingestor{producer: producer, logger: logger}
}

// Accept encodes env, produces it to mt.inbound (the durability boundary — §6.7/§7.3), and only then
// enqueues the accepted CDR row off the caller's path. It returns a flat sentinel the caller maps to
// its own surface — errs.ErrInternal on an encode fault, errs.ErrServiceUnavailable when the durable
// write fails — so a submission is never acknowledged before its record is durable. On success the
// caller may safely acknowledge with env.MessageID. The body is revealed only inside EncodeInbound,
// never here (invariant a).
func (i *Ingestor) Accept(ctx context.Context, env pipeline.InboundMT) error {
	rec, err := pipeline.EncodeInbound(env)
	if err != nil {
		i.logger.ErrorContext(ctx, "encode mt.inbound", "message_id", env.MessageID, "err", err)
		return fmt.Errorf("encode mt.inbound: %w", errs.ErrInternal)
	}

	if err := i.producer.Produce(ctx, rec); err != nil {
		i.logger.ErrorContext(ctx, "produce mt.inbound", "message_id", env.MessageID, "err", err)
		return fmt.Errorf("produce mt.inbound: %w", errs.ErrServiceUnavailable)
	}

	// Acknowledgement earned. The accepted CDR row is not written here: AcceptedConsumer projects it durably
	// off mt.inbound (step-101), so a saturation on the read-model write can never lose the row.
	return nil
}

// AcceptedRow builds the pre-dispatch accepted CDR row from the inbound envelope. The destination is left as
// submitted here: AcceptedConsumer normalizes it (the phone parse is too heavy to run inline at the ingest
// rate) to the same canonical form the router stores, so a message spells its destination the same across all
// its lifecycle rows. The body is not set here; AcceptedConsumer seals it into the content column per policy.
func AcceptedRow(env pipeline.InboundMT) clickhouse.CDRRow {
	return clickhouse.CDRRow{
		MessageID:    env.MessageID,
		TraceID:      env.TraceID,
		AccountID:    env.AccountID,
		CustomerID:   env.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   env.From,
		DestAddr:     env.To,
		SubmittedAt:  env.SubmittedAt,
		Status:       clickhouse.StatusAccepted,
		SegmentCount: 1,
		// Encoding is authoritative in the envelope (REST resolves it from its enum, an SMPP submit from
		// data_coding at ingestion), so the accepted row and the connector's enroute row agree.
		Encoding: clickhouse.EncodingOf(env.Encoding),
		Billed:   false,
	}
}
