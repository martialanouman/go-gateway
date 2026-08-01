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
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/martialanouman/go-gateway/internal/metricstream"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
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

// ContentSealer seals the message body into a CDR row per the customer's content_storage policy, so a
// rejected message stores its content the same way an accepted one does (a rejection is still the customer's
// message; rejects are also what operators most want to inspect). It never fails: an unavailable data key
// degrades to no content. A nil sealer disables content storage. *ingest.ContentSealer satisfies it; declared
// consumer-side. The body reaches only the content column, never a log (invariant a).
type ContentSealer interface {
	Seal(ctx context.Context, row *clickhouse.CDRRow, body msg.Body, customerID uuid.UUID)
}

// Deps are the router's collaborators. The tracer comes in through Deps (never the global) so the
// in-process E2E can wire several services with distinct tracers.
type Deps struct {
	Consumer Consumer
	Producer Producer
	Pipeline *pipeline.Pipeline
	CDR      CDRWriter
	Sealer   ContentSealer
	Tracer   trace.Tracer
	Logger   *slog.Logger
	// Stream feeds the realtime dashboard (metrics.stream, step-182). Optional and best-effort: a nil
	// Stream disables emission entirely, and no failure of it may ever reach a message.
	Stream StreamEmitter
	// Metrics is the Prometheus side of the same figures. Both surfaces are fed from ONE call site with ONE
	// set of names (step-180): a live dashboard and Grafana disagreeing on what a number is called makes it
	// impossible to correlate a spike with its history. Optional; nil disables it.
	Metrics *metrics.Catalog
}

// StreamEmitter records live figures for the realtime feed. It is declared here, consumer-side, and
// implemented by internal/metricstream. Its methods return nothing on purpose — the routing path must not be
// able to branch on a dashboard failure.
type StreamEmitter interface {
	Add(kind string, labels metricstream.Labels, delta float64)
	Observe(kind string, labels metricstream.Labels, seconds float64)
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
func (r *Router) handle(ctx context.Context, rec kafka.Record) (err error) {
	ctx, span := r.deps.Tracer.Start(ctx, "router.process")
	defer span.End()
	// The service root span must carry the failure too, not only the stage that produced it: with
	// error-biased sampling an unmarked parent is dropped by the ratio, leaving the stage span orphaned
	// with a parent_span_id resolving to nothing — and get-message-trace (step-185) reads that chain.
	defer func() { observability.RecordSpanError(span, err) }()

	in, err := pipeline.DecodeInbound(rec)
	if err != nil {
		// A malformed record is a poison message; there is no dead-letter path until M7, so fail and
		// let the operator see it rather than silently drop work.
		return fmt.Errorf("router: decode mt.inbound: %w", err)
	}

	started := time.Now()
	routed, segments, perr := r.deps.Pipeline.Process(ctx, in)
	elapsed := time.Since(started).Seconds()
	r.stream(func(s StreamEmitter) { s.Observe("pipeline_duration_seconds", nil, elapsed) })
	if r.deps.Metrics != nil {
		r.deps.Metrics.PipelineDuration.Observe(elapsed)
	}
	if perr != nil {
		code, ok := errs.CodeOf(perr)
		if !ok {
			// No platform code means an unexpected internal fault: treat as transient and retry.
			return fmt.Errorf("router: pipeline: %w", perr)
		}
		row := rejectedRow(in, code)
		// Seal the body into the rejected row per the customer's content policy (same as an accepted row).
		// Fail-open: an unavailable data key stores no content, never fails the rejection.
		if r.deps.Sealer != nil {
			r.deps.Sealer.Seal(ctx, &row, in.Body, in.CustomerID)
		}
		if err := r.deps.CDR.Insert(ctx, row); err != nil {
			return fmt.Errorf("router: write rejected cdr: %w", err)
		}
		r.deps.Logger.InfoContext(ctx, "message rejected in pipeline",
			"message_id", in.MessageID, "account_id", in.AccountID, "code", code)
		// The label values are a platform Code and a constant — a closed vocabulary from the code, which is
		// the rule the stream depends on (an emitter keyed by label values is a map that grows with them).
		r.stream(func(s StreamEmitter) {
			s.Add("messages_total", metricstream.Labels{"status": "rejected"}, 1)
			s.Add("rejected_total", metricstream.Labels{"code": string(code)}, 1)
		})
		if r.deps.Metrics != nil {
			r.deps.Metrics.MessagesTotal.WithLabelValues("rejected").Inc()
			r.deps.Metrics.RejectedTotal.WithLabelValues(string(code)).Inc()
		}
		return nil
	}

	// Fan out one mt.routed record per segment. Every record keeps the message's key (MessageID), so
	// all segments land on one partition and reach the same bind in order (§7.3). Produce is synchronous
	// and idempotent; the offset commits (this returns nil) only after ALL segments are acknowledged, so
	// a crash mid-fan-out reprocesses the whole message and re-produces byte-identical records (the
	// split is deterministic), which the SMSC/handset reassemble to the same place (same ref/seq/total).
	// The CDR is still message-grained here: its ORDER BY has no segment_seq, so the N per-segment rows
	// collapse under ReplacingMergeTree into one row per message reflecting the highest-version segment
	// outcome — a partially-failed multi-segment message can be mis-reported until per-segment CDR lands
	// (step-082c adds segment_seq to the CDR key and get-message aggregation).
	for _, seg := range segments {
		rec := routed
		rec.Body = msg.NewBody(seg.Payload) // audited: segment wire bytes ride the record value only
		rec.SegmentSeq = seg.Seq
		rec.SegmentCount = seg.Total
		rec.HasUDH = seg.HasUDH
		out, err := pipeline.EncodeRouted(rec)
		if err != nil {
			return fmt.Errorf("router: encode mt.routed: %w", err)
		}
		if err := r.deps.Producer.Produce(ctx, out); err != nil {
			return fmt.Errorf("router: publish mt.routed: %w", err)
		}
	}
	r.stream(func(s StreamEmitter) {
		s.Add("messages_total", metricstream.Labels{"status": "routed"}, 1)
	})
	if r.deps.Metrics != nil {
		r.deps.Metrics.MessagesTotal.WithLabelValues("routed").Inc()
	}
	return nil
}

// stream runs fn against the emitter when one is configured. One nil check in one place, so no call site can
// forget it and no emission can be mistaken for something the message path depends on.
func (r *Router) stream(fn func(StreamEmitter)) {
	if r.deps.Stream == nil {
		return
	}
	fn(r.deps.Stream)
}

// rejectedRow builds the CDR row for a message rejected before dispatch. It carries the immutable
// submitted_at from ingestion and the addresses as they stood at rejection (the destination may be
// un-normalised when E.164 itself failed). The body is not set here; the caller seals it into the content
// column per the customer's content policy (invariant a: the body reaches only that column, never a log).
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
		Encoding:     clickhouse.EncodingOf(in.Encoding),
		Billed:       false,
	}
}
