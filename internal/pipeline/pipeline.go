package pipeline

import (
	"context"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/platform/e164"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// Route is the outcome of route resolution: the connector to send through and the route that
// matched (nil for a default/catch-all route).
type Route struct {
	ConnectorID uuid.UUID
	RouteID     *uuid.UUID
}

// Resolver resolves a normalized destination to a connector. It is implemented over an immutable
// snapshot loaded at startup (internal/routing); the interface lives here, consumer-side.
type Resolver interface {
	Resolve(ctx context.Context, dest string) (Route, error)
}

// Pipeline runs the ordered MT stages the router applies to every message (spec §6.1). The order is
// frozen: a routing short-cut may skip route resolution, never a compliance stage. M2 implements
// E.164 normalization and declarative route resolution; the compliance and metering stages are
// explicit pass-through STUBs that still emit their span.
type Pipeline struct {
	tracer   trace.Tracer
	resolver Resolver
}

// New builds a Pipeline. Destinations are normalized to their canonical digits-only form; the
// public contract carries a full country code (the "+" being optional), so no default region is
// needed. See internal/platform/e164.
func New(tracer trace.Tracer, resolver Resolver) *Pipeline {
	return &Pipeline{tracer: tracer, resolver: resolver}
}

// Process runs the pipeline on an inbound message and returns the routed message. On a rejection it
// returns an error carrying a platform Code (invalid_destination, no_route, …); the caller records
// a rejected CDR row and does not publish to mt.routed. The body is passed through untouched and
// never appears in a span (invariant a).
func (p *Pipeline) Process(ctx context.Context, in InboundMT) (RoutedMT, error) {
	out := RoutedMT{
		MessageID:          in.MessageID,
		TraceID:            in.TraceID,
		AccountID:          in.AccountID,
		CustomerID:         in.CustomerID,
		From:               in.From,
		To:                 in.To,
		Body:               in.Body,
		RegisteredDelivery: in.RegisteredDelivery,
		ValidityPeriod:     in.ValidityPeriod,
		SubmittedAt:        in.SubmittedAt,
		SegmentCount:       1,
	}

	// 1. E.164 normalization of the destination. Source normalization is a sender-id concern (M5),
	// so From is left as the client sent it.
	if err := p.stage(ctx, "pipeline.e164", func(context.Context) error {
		norm, err := e164.Normalize(in.To)
		if err != nil {
			return errs.ErrInvalidDestination
		}
		out.To = norm
		return nil
	}); err != nil {
		return RoutedMT{}, err
	}

	// 2. STUB M5: sender-ID authorization — pass-through until M5. See plan §8.
	p.stubStage(ctx, "pipeline.sender_id")
	// 3. STUB M5: opt-out / suppression — pass-through until M5. See plan §8.
	p.stubStage(ctx, "pipeline.opt_out")
	// 4. STUB M6: anti-spam — pass-through until M6. See plan §8.
	p.stubStage(ctx, "pipeline.anti_spam")

	// 5. Route resolution (declarative static only in M2). A short-cut here would skip only this
	// stage, never the compliance stages above (spec §6.1).
	if err := p.stage(ctx, "pipeline.route", func(ctx context.Context) error {
		route, err := p.resolver.Resolve(ctx, out.To)
		if err != nil {
			return err
		}
		out.ConnectorID = route.ConnectorID
		out.RouteID = route.RouteID
		return nil
	}); err != nil {
		return RoutedMT{}, err
	}

	// 6. Encoding / segmentation. M2 assumes a single segment; auto resolves to GSM-7. Real
	// detection and UDH segmentation land with the encoding milestone.
	if err := p.stage(ctx, "pipeline.encoding", func(context.Context) error {
		out.Encoding = resolveEncoding(in.Encoding)
		out.SegmentCount = 1
		return nil
	}); err != nil {
		return RoutedMT{}, err
	}

	// 7. STUB M7: rate limit — pass-through until M7. See plan §8.
	p.stubStage(ctx, "pipeline.rate_limit")
	// 8. STUB (billing): MT credit reserve — pass-through until billing lands. See plan §8.
	p.stubStage(ctx, "pipeline.credit")

	return out, nil
}

// stage runs one pipeline step under its own span. The span records a rejection's code (never a
// body — stage errors carry only platform Codes and identifiers).
func (p *Pipeline) stage(ctx context.Context, name string, fn func(context.Context) error) error {
	ctx, span := p.tracer.Start(ctx, name)
	defer span.End()
	if err := fn(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// stubStage emits the span of a not-yet-implemented stage. A STUB is never silent: it appears in the
// trace exactly like a real stage, so the pipeline's shape is visible before its logic exists
// (plan §0.3).
func (p *Pipeline) stubStage(ctx context.Context, name string) {
	_, span := p.tracer.Start(ctx, name)
	span.End()
}

// resolveEncoding maps the requested encoding to the resolved one the CDR records. M2 does not
// auto-detect: auto (and anything unrecognised) resolves to GSM-7.
func resolveEncoding(requested string) string {
	switch requested {
	case "gsm7", "ucs2", "binary":
		return requested
	default:
		return "gsm7"
	}
}
