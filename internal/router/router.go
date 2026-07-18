// Package router runs the MT pipeline: it consumes mt.inbound, normalises and routes each message,
// and publishes mt.routed. A message rejected by the pipeline (bad destination, no route) gets a
// rejected CDR row here — it never reaches the connector — so get-message reflects the outcome
// instead of resting at accepted forever. The happy path publishes mt.routed and lets
// connector-pool-svc write the enroute row.
package router

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// Consumer reads mt.inbound. *kafka.Consumer satisfies it.
type Consumer interface {
	Run(ctx context.Context, handle kafka.Handler) error
}

// Producer publishes mt.routed. *kafka.Producer satisfies it.
type Producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// CDRWriter records a rejection. *clickhouse.CDRWriter satisfies it.
type CDRWriter interface {
	Insert(ctx context.Context, row clickhouse.CDRRow) error
}

// Deps are the router's collaborators. The tracer comes in through Deps (never the global) so the
// in-process E2E can wire several services with distinct tracers.
type Deps struct {
	Consumer Consumer
	Producer Producer
	Pipeline *pipeline.Pipeline
	CDR      CDRWriter
	Tracer   trace.Tracer
	Logger   *slog.Logger
}

// Router is the MT routing service.
type Router struct {
	deps Deps
}

// New builds a Router. A nil logger defaults to slog.Default; a nil tracer would panic on first
// span, so the caller must supply one (cmd/main installs it; tests use the recorder's).
func New(deps Deps) *Router {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &Router{deps: deps}
}

// Run consumes mt.inbound until ctx is cancelled. It returns whatever the consumer returns: nil on a
// clean stop, an error on a transient fault (which restarts the service and reprocesses).
func (r *Router) Run(ctx context.Context) error {
	return r.deps.Consumer.Run(ctx, r.handle)
}

// handle processes one mt.inbound record. Returning nil commits the offset; returning an error
// leaves it uncommitted for reprocessing (at-least-once). A pipeline rejection is a terminal
// outcome — the CDR row is written and the offset committed; only an infrastructure fault (produce
// or CDR write failure) returns an error.
func (r *Router) handle(ctx context.Context, rec kafka.Record) error {
	ctx, span := r.deps.Tracer.Start(ctx, "router.process")
	defer span.End()

	in, err := pipeline.DecodeInbound(rec)
	if err != nil {
		// A malformed record is a poison message; there is no dead-letter path until M7, so fail and
		// let the operator see it rather than silently drop work.
		return fmt.Errorf("router: decode mt.inbound: %w", err)
	}

	routed, perr := r.deps.Pipeline.Process(ctx, in)
	if perr != nil {
		code, ok := errs.CodeOf(perr)
		if !ok {
			// No platform code means an unexpected internal fault: treat as transient and retry.
			return fmt.Errorf("router: pipeline: %w", perr)
		}
		if err := r.deps.CDR.Insert(ctx, rejectedRow(in, code)); err != nil {
			return fmt.Errorf("router: write rejected cdr: %w", err)
		}
		r.deps.Logger.InfoContext(ctx, "message rejected in pipeline",
			"message_id", in.MessageID, "account_id", in.AccountID, "code", code)
		return nil
	}

	out, err := pipeline.EncodeRouted(routed)
	if err != nil {
		return fmt.Errorf("router: encode mt.routed: %w", err)
	}
	if err := r.deps.Producer.Produce(ctx, out); err != nil {
		return fmt.Errorf("router: publish mt.routed: %w", err)
	}
	return nil
}

// rejectedRow builds the CDR row for a message rejected before dispatch. It carries the immutable
// submitted_at from ingestion and the addresses as they stood at rejection (the destination may be
// un-normalised when E.164 itself failed). The body is never included (invariant a).
func rejectedRow(in pipeline.InboundMT, code errs.Code) clickhouse.CDRRow {
	errorCode := string(code)
	return clickhouse.CDRRow{
		MessageID:    in.MessageID,
		TraceID:      in.TraceID,
		AccountID:    in.AccountID,
		CustomerID:   in.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   in.From,
		DestAddr:     in.To,
		SubmittedAt:  in.SubmittedAt,
		Status:       clickhouse.StatusRejected,
		ErrorCode:    &errorCode,
		SegmentCount: 1,
		Encoding:     mapEncoding(in.Encoding),
		Billed:       false,
	}
}

// mapEncoding projects a requested encoding onto the CDR enum. auto (and anything unrecognised)
// resolves to GSM-7, matching the pipeline's M2 behaviour.
func mapEncoding(requested string) clickhouse.Encoding {
	switch requested {
	case "ucs2":
		return clickhouse.EncodingUCS2
	case "binary":
		return clickhouse.EncodingBinary
	default:
		return clickhouse.EncodingGSM7
	}
}
