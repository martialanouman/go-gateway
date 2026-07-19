// Package ingest is the one MT ingestion path shared by every submission surface. A REST submit and
// an SMPP submit_sm both build a pipeline.InboundMT and hand it to Ingestor.Accept, which encodes it,
// writes it durably to mt.inbound (the durability boundary that earns the acknowledgement), and then
// projects the accepted CDR row off the caller's path. Both protocols reaching the pipeline through
// this single helper is what makes REST/SMPP parity a property of the code, not a coincidence.
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

// Ingestor performs the shared ingestion sequence for one submission. It holds the durable producer
// and the off-path accepted-row writer; the per-protocol surfaces build the envelope and map the
// result to their own response.
type Ingestor struct {
	producer Producer
	accepted *AcceptedWriter
	logger   *slog.Logger
}

// NewIngestor wires an Ingestor over a producer and the accepted-row writer.
func NewIngestor(producer Producer, accepted *AcceptedWriter, logger *slog.Logger) *Ingestor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Ingestor{producer: producer, accepted: accepted, logger: logger}
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

	// Acknowledgement earned. The accepted CDR row is written asynchronously, never blocking the caller.
	i.accepted.Enqueue(AcceptedRow(env))
	return nil
}

// AcceptedRow builds the pre-dispatch accepted CDR row from the inbound envelope. The destination is
// left as submitted here: the AcceptedWriter normalizes it off the request path (the phone parse is
// too heavy to run inline at the ingest rate), to the same canonical form the router stores, so a
// message spells its destination the same across all its lifecycle rows. The body is never included
// (invariant a).
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
		Encoding:     clickhouse.EncodingOf(env.Encoding),
		Billed:       false,
	}
}
