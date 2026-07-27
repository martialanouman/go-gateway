package pipeline

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	pipeenc "github.com/martialanouman/go-gateway/internal/pipeline/encoding"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
	"github.com/martialanouman/go-gateway/internal/platform/encoding"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// Route is the outcome of route resolution: the connector to send through and the route that
// matched (nil for a default/catch-all route).
type Route struct {
	ConnectorID uuid.UUID
	RouteID     *uuid.UUID
	// FallbackChain is the ordered connector fallback order for this route (failover_priority /
	// least_loaded), carried to mt.routed so the connector pool can reroute unilaterally (step-125).
	// Empty for strategies without a meaningful fallback order.
	FallbackChain []uuid.UUID
}

// RouteRequest is the context route resolution needs: the normalized destination plus the message
// metadata a routing script reads (§6.1). It carries NO message body (invariant a).
type RouteRequest struct {
	Dest       string // normalized E.164 destination
	From       string
	AccountID  uuid.UUID
	CustomerID uuid.UUID
	// Segments is always 1 at route resolution: segmentation runs later in the pipeline (frozen order,
	// §6.1), so the real segment count is not yet known. A script must not route on it.
	Segments int
	// ReceivedAtMs is the message's immutable accept time (SubmittedAt) in epoch milliseconds, so a
	// script can route on time-of-day deterministically (no real clock is exposed to the script).
	ReceivedAtMs int64
}

// Resolver resolves a route request to a connector. It is implemented over an immutable snapshot
// loaded at startup (internal/routing) — exact-number short-cut, then routing script, then the
// declarative resolver; the interface lives here, consumer-side.
type Resolver interface {
	Resolve(ctx context.Context, req RouteRequest) (Route, error)
}

// SenderIDAuthorizer authorizes a message's source address against the account's sender-ID policy and
// its customer's registered sender IDs (spec §6.19). It is implemented over an immutable snapshot
// (internal/pipeline/senderid); the interface lives here, consumer-side. A rejection returns
// errs.ErrSenderIDNotAuthorized.
type SenderIDAuthorizer interface {
	Authorize(ctx context.Context, accountID, customerID uuid.UUID, from string) error
}

// OptOutChecker reports whether an MT's destination is suppressed (opted out) in any scope applicable
// to the message — platform, customer, account, or the sending inbound number (spec §6.20). It is
// implemented over an immutable Bloom snapshot with exact confirmation (internal/pipeline/optout);
// the interface lives here, consumer-side. dest is the normalized destination. The (accountID,
// customerID) order matches SenderIDAuthorizer.Authorize so the two compliance stages read alike. A
// non-nil error is a transient fault (the exact confirmation store) the caller must not treat as
// "not suppressed".
type OptOutChecker interface {
	IsOptedOut(ctx context.Context, accountID, customerID uuid.UUID, from, dest string) (bool, error)
}

// AntispamEvaluator evaluates a message against the active anti-spam rules and returns the action to
// take — block, flag, throttle, or empty (no match) — per spec §6.20. It is implemented over an
// immutable rule snapshot with Redis-backed velocity/duplicate/reputation checks
// (internal/pipeline/antispam); the interface lives here, consumer-side. body is the revealed message
// body, read in memory only (invariant a). The Redis-backed checks FAIL OPEN (§1.5): a store fault
// flags the message rather than blocking it, so the error return is currently always nil (retained
// for interface stability).
type AntispamEvaluator interface {
	Evaluate(ctx context.Context, accountID, customerID uuid.UUID, from, dest string, body []byte) (cp.AntispamAction, error)
}

// RateLimiter applies the account >= route >= connector throughput limits to a message of `segments`
// segments, consuming that many tokens from each applicable bucket (spec §6.4). It returns
// errs.ErrRateLimited when a limit is exceeded; the connector's technical ceiling is never crossed. It
// is implemented over an immutable snapshot plus a Redis token bucket (internal/pipeline/ratelimit) that
// fails closed on a store outage; the interface lives here, consumer-side. A nil RateLimiter disables
// the stage (the pre-M6 pass-through).
type RateLimiter interface {
	Check(ctx context.Context, accountID, connectorID uuid.UUID, routeID *uuid.UUID, segments int) error
}

// Pipeline runs the ordered MT stages the router applies to every message (spec §6.1). The order is
// frozen: a routing short-cut may skip route resolution, never a compliance stage. It implements
// E.164 normalization, sender-ID authorization (M5), declarative route resolution, encoding
// resolution, UDH segmentation and rate limiting (M6); the remaining metering stage (credit) is an
// explicit pass-through STUB that still emits its span.
type Pipeline struct {
	tracer      trace.Tracer
	resolver    Resolver
	senderIDs   SenderIDAuthorizer
	optOut      OptOutChecker
	antispam    AntispamEvaluator
	rateLimiter RateLimiter
}

// New builds a Pipeline. Destinations are normalized to their canonical digits-only form; the
// public contract carries a full country code (the "+" being optional), so no default region is
// needed. See internal/platform/e164. A nil rateLimiter leaves the rate-limit stage a pass-through.
func New(tracer trace.Tracer, resolver Resolver, senderIDs SenderIDAuthorizer, optOut OptOutChecker, antispam AntispamEvaluator, rateLimiter RateLimiter) *Pipeline {
	return &Pipeline{tracer: tracer, resolver: resolver, senderIDs: senderIDs, optOut: optOut, antispam: antispam, rateLimiter: rateLimiter}
}

// Process runs the pipeline on an inbound message and returns the routed template plus the segments
// it was split into — one mt.routed record per segment (the router fans them out, all under the same
// partition key so they stay ordered on one bind). On a rejection it returns an error carrying a
// platform Code (invalid_destination, no_route, …); the caller records a rejected CDR row and does not
// publish. The returned template's own Body is the original message; each segment carries its own wire
// short_message in Segment.Payload. The body is read in memory only for encoding and segmentation and
// never appears in a span (invariant a).
func (p *Pipeline) Process(ctx context.Context, in InboundMT) (RoutedMT, []pipeenc.Segment, error) {
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
		DataCoding:         in.DataCoding,
		SubmittedAt:        in.SubmittedAt,
		SegmentCount:       1,
		// Client traffic is always billable; only a system message (a STOP auto-reply produced straight
		// to mt.routed, §6.20) is not — and that path never runs this pipeline.
		Billable: true,
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
		return RoutedMT{}, nil, err
	}

	// 2. Sender-ID authorization (§6.19). A frozen compliance stage: never short-circuited by an exact
	// route (invariant b). The span carries only the rejection code, never the body (invariant a).
	if err := p.stage(ctx, "pipeline.sender_id", func(ctx context.Context) error {
		return p.senderIDs.Authorize(ctx, in.AccountID, in.CustomerID, in.From)
	}); err != nil {
		return RoutedMT{}, nil, err
	}

	// 3. Opt-out / suppression (§6.20). A frozen compliance stage, never short-circuited by an exact
	// route (invariant b). Blocks if the destination is suppressed in ANY applicable scope. The span
	// carries only the rejection code, never the body (invariant a).
	if err := p.stage(ctx, "pipeline.opt_out", func(ctx context.Context) error {
		optedOut, err := p.optOut.IsOptedOut(ctx, in.AccountID, in.CustomerID, in.From, out.To)
		if err != nil {
			return err
		}
		if optedOut {
			return errs.ErrRecipientOptedOut
		}
		return nil
	}); err != nil {
		return RoutedMT{}, nil, err
	}

	// 4. Anti-spam (§6.20). A frozen compliance stage, never short-circuited by an exact route
	// (invariant b). Content is read in memory only — the span carries the action, never the body
	// (invariant a). block rejects; flag/throttle annotate the span without stopping the message.
	if err := p.stage(ctx, "pipeline.anti_spam", func(ctx context.Context) error {
		action, err := p.antispam.Evaluate(ctx, in.AccountID, in.CustomerID, in.From, out.To, in.Body.Reveal())
		if err != nil {
			return err
		}
		if action == cp.AntispamActionBlock {
			return errs.ErrContentBlocked
		}
		if action != "" {
			trace.SpanFromContext(ctx).SetAttributes(attribute.String("anti_spam.action", string(action)))
		}
		return nil
	}); err != nil {
		return RoutedMT{}, nil, err
	}

	// 5. Route resolution (declarative static only in M2). A short-cut here would skip only this
	// stage, never the compliance stages above (spec §6.1).
	if err := p.stage(ctx, "pipeline.route", func(ctx context.Context) error {
		route, err := p.resolver.Resolve(ctx, RouteRequest{
			Dest: out.To, From: in.From, AccountID: in.AccountID, CustomerID: in.CustomerID,
			Segments: out.SegmentCount, ReceivedAtMs: in.SubmittedAt.UnixMilli(),
		})
		if err != nil {
			return err
		}
		out.ConnectorID = route.ConnectorID
		out.RouteID = route.RouteID
		out.FallbackChain = route.FallbackChain
		return nil
	}); err != nil {
		return RoutedMT{}, nil, err
	}

	// 6. Encoding (§6.6). Resolve the wire encoding (GSM-7 / UCS-2 / binary) and count the segments.
	// The body is read in memory only for detection, never logged (invariant a). A client that drove
	// the DCS directly (data_coding, always set on the SMPP path) fixes the charset the message is both
	// sent AND segmented in, so it takes precedence: segmenting in a different charset than the wire
	// byte would size the segments wrong. Otherwise the requested encoding enum (or auto-detect) wins.
	// A connector data_coding_default is a later wiring (nil for now).
	body := in.Body.Reveal() // audited: body -> in-memory encoding/segmentation only, never logged
	if err := p.stage(ctx, "pipeline.encoding", func(context.Context) error {
		// Only the encoding matters here; the segment stage below is the authority on SegmentCount
		// (DetectAndCount's own count would be wrong for a pre-segmented body anyway).
		out.Encoding, _ = encoding.DetectAndCount(requestedEncoding(in), nil, body)
		return nil
	}); err != nil {
		return RoutedMT{}, nil, err
	}

	// 7. Segmentation (§6.6). Split the body into the concatenated segments the SMSC wire carries, one
	// mt.routed record each (the router fans them out). It precedes rate-limit and credit so those meter
	// per segment (step-084/085). A client that pre-segmented its own SMPP submit (esm_class UDH
	// indicator already set) is never re-split: its body already carries a UDH and travels whole. The
	// span carries only the segment count, never the body (invariant a).
	var segments []pipeenc.Segment
	if err := p.stage(ctx, "pipeline.segment", func(ctx context.Context) error {
		if in.ESMClass&smpp.ESMClassUDHIndicator != 0 {
			segments = []pipeenc.Segment{{Seq: 1, Total: 1, Payload: body, HasUDH: true}}
		} else {
			segments = pipeenc.Split(in.MessageID, body, out.Encoding)
		}
		out.SegmentCount = len(segments)
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int("segment.count", len(segments)))
		return nil
	}); err != nil {
		return RoutedMT{}, nil, err
	}

	// 8. Rate limit (§6.4). Consume this message's segments from the account, route and connector
	// buckets in precedence order; a breach rejects with rate_limited (the caller writes a rejected CDR,
	// never sends). It comes AFTER segmentation so the cost is the real segment count, and BEFORE the
	// credit reserve and SMSC send. A routing short-cut (M7) would skip route resolution, never this
	// stage. The span carries no body (invariant a). A nil limiter is a pass-through (pre-M6).
	if err := p.stage(ctx, "pipeline.rate_limit", func(ctx context.Context) error {
		if p.rateLimiter == nil {
			return nil
		}
		return p.rateLimiter.Check(ctx, out.AccountID, out.ConnectorID, out.RouteID, out.SegmentCount)
	}); err != nil {
		return RoutedMT{}, nil, err
	}

	// 9. STUB (billing): MT credit reserve — pass-through until billing lands. See plan §8.
	p.stubStage(ctx, "pipeline.credit")

	return out, segments, nil
}

// requestedEncoding resolves the encoding request the detector sees. A client-supplied data_coding
// (always set on the SMPP path, optional on REST) is the charset the message will be sent in, so it
// dictates how the message is segmented too; its charset is derived through the shared FromDataCoding
// vocabulary. Without one, the requested encoding enum (auto|gsm7|ucs2|binary) is used as before.
func requestedEncoding(in InboundMT) string {
	if dc := in.DataCoding; dc != nil && *dc >= 0 && *dc <= 255 {
		return encoding.FromDataCoding(uint8(*dc)) //nolint:gosec // bounded to 0..255 on the line above
	}
	return in.Encoding
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
