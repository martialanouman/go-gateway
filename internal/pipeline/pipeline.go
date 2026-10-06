package pipeline

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
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
	// Rank is the traffic category rank (ADR-0020 §4): a connector reserved above it is skipped.
	Rank int
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

// SenderIDAuthorizer authorizes a message's source address against its customer's active registered
// sender IDs (spec §6.19, ADR-0020) and returns the traffic category declared for it. It is implemented
// over an immutable snapshot (internal/pipeline/senderid); the interface lives here, consumer-side. A
// rejection returns errs.ErrSenderIDNotAuthorized.
type SenderIDAuthorizer interface {
	Authorize(ctx context.Context, customerID uuid.UUID, from string) (cp.TrafficCategory, error)
}

// OptOutChecker reports whether an MT's destination is suppressed (opted out) in any scope applicable
// to the message — platform, customer, account, or the sending inbound number (spec §6.20). It is
// implemented over an immutable Bloom snapshot with exact confirmation (internal/pipeline/optout);
// the interface lives here, consumer-side. dest is the normalized destination. A non-nil error is a transient fault (the exact confirmation store) the caller must not treat as
// "not suppressed".
type OptOutChecker interface {
	IsOptedOut(ctx context.Context, accountID, customerID uuid.UUID, from, dest string) (bool, error)
}

// AntispamEvaluator evaluates a message against the active anti-spam rules and returns the action to
// take — block, flag, throttle, or empty (no match) — per spec §6.20. It is implemented over an
// immutable rule snapshot with Redis-backed velocity/duplicate/reputation checks, and checks the traffic
// against the sender's declared category (category_mismatch)
// (internal/pipeline/antispam); the interface lives here, consumer-side. body is the revealed message
// body, read in memory only (invariant a). The Redis-backed checks FAIL OPEN (§1.5): a store fault
// flags the message rather than blocking it, so the error return is currently always nil (retained
// for interface stability).
type AntispamEvaluator interface {
	Evaluate(ctx context.Context, messageID, accountID, customerID uuid.UUID, from string, category cp.TrafficCategory, dest string, body []byte) (cp.AntispamAction, error)
}

// CreditReserver reserves MT credit for a message before the SMSC send (§6.9, step-145). reserved reports
// whether a reservation now exists: false means billing is disabled for the customer — a cached-boolean
// decision that makes NO billing round-trip — so nothing is settled downstream. When reserved, ownerType is
// the balance owner the reservation was made against (customer | smpp_account), pinned onto mt.routed so
// connector-pool captures the identical key (step-146). A business denial (insufficient funds) returns
// errs.ErrInsufficientCredit (a coded reject → rejected CDR, HTTP 402); a transport fault is returned raw so
// the caller retries (fail-closed: a billed message is not sent until billing answers). It is implemented
// over an immutable per-customer snapshot plus the billing gRPC client (internal/pipeline/credit); the
// interface lives here, consumer-side. A nil CreditReserver disables the stage (the pre-billing pass-through).
type CreditReserver interface {
	Reserve(ctx context.Context, accountID, customerID, messageID uuid.UUID, segments int) (reserved bool, ownerType string, err error)
}

// Deps are the pipeline's collaborators, one per stage that needs one. They are named rather than
// positional on purpose: a nil Credit turns its stage into a pass-through, and as positional arguments
// such a nil was indistinguishable from padding — the reference load harness
// ran for two steps against a pipeline silently amputated of its rate-limit, credit and Redis-backed
// anti-spam stages, and measured it as if it were production (step-201d). Tracer is required; the
// others are required unless their godoc says otherwise.
type Deps struct {
	Tracer    trace.Tracer
	Resolver  Resolver
	SenderIDs SenderIDAuthorizer
	OptOut    OptOutChecker
	Antispam  AntispamEvaluator
	// Credit is optional: nil leaves the credit stage a pass-through (the pre-billing behaviour).
	Credit CreditReserver
}

// Pipeline runs the ordered MT stages the router applies to every message (spec §6.1). The order is
// frozen: a routing short-cut may skip route resolution, never a compliance stage. It implements
// E.164 normalization, sender-ID authorization (M5), declarative route resolution, encoding
// resolution, UDH segmentation, rate limiting (M6) and the opt-in credit reserve (M9, step-145).
type Pipeline struct {
	deps Deps
}

// New builds a Pipeline. Destinations are normalized to their canonical digits-only form; the
// public contract carries a full country code (the "+" being optional), so no default region is
// needed. See internal/platform/e164.
func New(deps Deps) *Pipeline {
	return &Pipeline{deps: deps}
}

// Process runs the pipeline on an inbound message and returns the routed template plus the segments
// it was split into — one mt.routed record per segment (the router fans them out, all under the same
// partition key so they stay ordered on one bind). On a rejection it returns an error carrying a
// platform Code (invalid_destination, no_route, …); the caller records a rejected CDR row and does not
// publish. The returned template's own Body is the original message; each segment carries its own wire
// short_message in Segment.Payload. The body is read in memory only for encoding and segmentation and
// never appears in a span (invariant a).
// With the error, the template carries what was known up to the failing stage: from the sender-ID stage
// on, the category and effective priority the rejected CDR row records (step-293).
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
		// Billable means "a reservation exists" and is set by the credit stage below only when billing is
		// enabled for the customer AND the reserve succeeds. It starts false: a billing-disabled customer
		// (or a nil credit stage, pre-billing) produces an unbilled mt.routed with nothing to settle.
		Billable: false,
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
	var category cp.TrafficCategory
	if err := p.stage(ctx, "pipeline.sender_id", func(ctx context.Context) error {
		var err error
		category, err = p.deps.SenderIDs.Authorize(ctx, in.CustomerID, in.From)
		return err
	}); err != nil {
		return RoutedMT{}, nil, err
	}
	out.TrafficCategory = category
	out.Priority = category.EffectivePriority(in.Priority)

	// 3. Opt-out / suppression (§6.20). A frozen compliance stage, never short-circuited by an exact
	// route (invariant b). Blocks if the destination is suppressed in ANY applicable scope. The span
	// carries only the rejection code, never the body (invariant a).
	if err := p.stage(ctx, "pipeline.opt_out", func(ctx context.Context) error {
		optedOut, err := p.deps.OptOut.IsOptedOut(ctx, in.AccountID, in.CustomerID, in.From, out.To)
		if err != nil {
			return err
		}
		if optedOut {
			return errs.ErrRecipientOptedOut
		}
		return nil
	}); err != nil {
		return out, nil, err
	}

	// 4. Anti-spam (§6.20). A frozen compliance stage, never short-circuited by an exact route
	// (invariant b). Content is read in memory only — the span carries the action, never the body
	// (invariant a). block rejects; flag/throttle annotate the span without stopping the message.
	if err := p.stage(ctx, "pipeline.anti_spam", func(ctx context.Context) error {
		action, err := p.deps.Antispam.Evaluate(ctx, in.MessageID, in.AccountID, in.CustomerID, in.From, category, out.To, in.Body.Reveal())
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
		return out, nil, err
	}

	// 5. Route resolution (declarative static only in M2). A short-cut here would skip only this
	// stage, never the compliance stages above (spec §6.1).
	if err := p.stage(ctx, "pipeline.route", func(ctx context.Context) error {
		route, err := p.deps.Resolver.Resolve(ctx, RouteRequest{
			Dest: out.To, From: in.From, AccountID: in.AccountID, CustomerID: in.CustomerID, Rank: category.Rank(),
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
		return out, nil, err
	}

	// 6. Encoding (§6.6). Resolve the wire encoding (GSM-7 / UCS-2 / binary) and count the segments.
	// The body is read in memory only for detection, never logged (invariant a). A client that drove
	// the DCS directly (data_coding, always set on the SMPP path) fixes the charset the message is both
	// sent AND segmented in, so it takes precedence: segmenting in a different charset than the wire
	// byte would size the segments wrong. Otherwise the requested encoding enum (or auto-detect) wins.
	// A connector data_coding_default is a later wiring (nil for now).
	body := in.Body.Reveal() // audited: body -> in-memory encoding/segmentation only, never logged
	if err := p.stage(ctx, "pipeline.encoding", func(context.Context) error {
		out.Encoding = wireEncoding(in, body)
		return nil
	}); err != nil {
		return out, nil, err
	}

	// 7. Segmentation (§6.6). Split the body into the concatenated segments the SMSC wire carries, one
	// mt.routed record each (the router fans them out). It precedes credit so that meters per segment.
	// The span carries only the segment count, never the body (invariant a).
	var segments []pipeenc.Segment
	if err := p.stage(ctx, "pipeline.segment", func(ctx context.Context) error {
		segments = split(in, body, out.Encoding)
		out.SegmentCount = len(segments)
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int("segment.count", len(segments)))
		return nil
	}); err != nil {
		return out, nil, err
	}

	// 8. Credit reserve (§6.9). Reserve this message's segments against the customer's balance, AFTER
	// segmentation (so the cost is the real segment count) and BEFORE the SMSC send. Billing is opt-in: a
	// disabled customer is skipped with ZERO billing round-trip (a cached-boolean decision). Insufficient
	// funds rejects with insufficient_credit (the caller writes a rejected CDR, never sends, no ledger); a
	// transport fault is returned raw so the router retries (fail-closed). A successful reserve pins Billable
	// and the resolved owner onto the routed message for connector-pool to capture (step-146). The span
	// carries no body (invariant a). A nil reserver is a pass-through (pre-billing).
	if err := p.stage(ctx, "pipeline.credit", func(ctx context.Context) error {
		if p.deps.Credit == nil {
			return nil
		}
		reserved, ownerType, err := p.deps.Credit.Reserve(ctx, out.AccountID, out.CustomerID, out.MessageID, out.SegmentCount)
		if err != nil {
			return err
		}
		if reserved {
			out.Billable = true
			out.OwnerType = ownerType
		}
		return nil
	}); err != nil {
		return out, nil, err
	}

	return out, segments, nil
}

// SegmentCount is the number of segments in's body occupies on the wire: what its admission costs before
// the acknowledgement (step-283). It is the pipeline's own encoding and segmentation, so the door and the
// wire meter the same figure.
func SegmentCount(in InboundMT) int {
	body := in.Body.Reveal() // audited: body -> in-memory segment count only, never logged
	return len(split(in, body, wireEncoding(in, body)))
}

func wireEncoding(in InboundMT, body []byte) string {
	enc, _ := encoding.DetectAndCount(requestedEncoding(in), nil, body)
	return enc
}

// split never re-splits a body the client pre-segmented (esm_class UDH indicator set): it already
// carries a UDH and travels whole.
func split(in InboundMT, body []byte, enc string) []pipeenc.Segment {
	if in.ESMClass&smpp.ESMClassUDHIndicator != 0 {
		return []pipeenc.Segment{{Seq: 1, Total: 1, Payload: body, HasUDH: true}}
	}
	return pipeenc.Split(in.MessageID, body, enc)
}

// requestedEncoding resolves the encoding request the detector sees. A client-supplied data_coding
// (always set on the SMPP path, optional on REST) is the charset the message will be sent in, so it
// dictates how the message is segmented too; its charset is derived through the shared FromDataCoding
// vocabulary. Without one, the requested encoding enum (auto|gsm7|ucs2|binary) is used as before.
func requestedEncoding(in InboundMT) string {
	if dc := in.DataCoding; dc != nil && *dc >= 0 && *dc <= 255 {
		return encoding.FromDataCoding(uint8(*dc))
	}
	return in.Encoding
}

// stage runs one pipeline step under its own span. A failure is recorded through the shared barrier, so the
// span carries the flat rejection code and never an arbitrary error's text (invariant a).
func (p *Pipeline) stage(ctx context.Context, name string, fn func(context.Context) error) error {
	ctx, span := p.deps.Tracer.Start(ctx, name)
	defer span.End()
	if err := fn(ctx); err != nil {
		observability.RecordSpanError(span, err)
		return err
	}
	return nil
}
