package modlrrouter

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// Producer publishes the resolved MO on mo.routed. *kafka.Producer satisfies it.
type Producer interface {
	Produce(ctx context.Context, rec kafka.Record) error
}

// UnroutedWriter records an MO that resolved to no account. *postgres.UnroutedMORepo satisfies it.
type UnroutedWriter interface {
	Create(ctx context.Context, in cp.NewUnroutedMO) (cp.UnroutedMO, error)
}

// UnroutedMetric counts unrouted MO, labelled by connector and reason. A wrapper over a
// prometheus.CounterVec satisfies it; New defaults a nil one to a no-op.
type UnroutedMetric interface {
	Inc(connectorID, reason string)
}

type noopMetric struct{}

func (noopMetric) Inc(string, string) {}

// MODeps are the MO router's collaborators.
type MODeps struct {
	Consumer Consumer
	Snapshot *Snapshot
	Producer Producer
	Unrouted UnroutedWriter
	Metric   UnroutedMetric
	// Stop applies opt-out keywords (STOP/START/HELP) to each MO (step-063). Optional: a nil detector
	// disables opt-out detection (the MO is still routed).
	Stop *StopDetector
	// Velocity records each MO into its source's velocity counter (step-066), so a sender's inbound MO
	// count feeds the anti-spam velocity a later MT is checked against. Optional and best-effort: a
	// recording failure never interrupts MO delivery.
	Velocity MOVelocityRecorder
	Tracer   trace.Tracer
	Logger   *slog.Logger
}

// MOVelocityRecorder records one inbound MO from a source into its velocity counter.
// A small adapter over *antispam.RedisState satisfies it.
type MOVelocityRecorder interface {
	RecordSource(ctx context.Context, from string) error
}

// MORouter resolves mobile-originated messages to an account and publishes the delivery intent.
type MORouter struct {
	deps MODeps
}

// NewMORouter builds an MO router. A nil logger defaults to slog.Default, a nil metric to a no-op.
func NewMORouter(deps MODeps) *MORouter {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Metric == nil {
		deps.Metric = noopMetric{}
	}
	return &MORouter{deps: deps}
}

// Run consumes mo.inbound until ctx is cancelled.
func (r *MORouter) Run(ctx context.Context) error {
	return r.deps.Consumer.Run(ctx, r.handler())
}

func (r *MORouter) handler() kafka.Handler {
	return func(ctx context.Context, rec kafka.Record) error {
		ctx, span := r.deps.Tracer.Start(ctx, "mo.route")
		defer span.End()

		mo, err := pipeline.DecodeMO(rec)
		if err != nil {
			// An undecodable record is permanently bad: log and commit, never wedge the partition.
			r.deps.Logger.ErrorContext(ctx, "modlrrouter: undecodable mo.inbound record, skipping", "err", err)
			return nil
		}

		dest := normalizeAddr(mo.To)
		// Reveal the body ONCE here: to match keywords in memory and to derive a deterministic message
		// id (below). It is never logged or spanned (invariant a); the matched account_id identifies the
		// outcome. A one-way hash of the body in the id derivation is not the body.
		body := mo.Body.Reveal()
		res := r.deps.Snapshot.resolve(dest, body)

		// Opt-out / STOP detection (§6.20) runs on any MO to a known, ACTIVE inbound number — routed or
		// not — and NEVER interrupts delivery: it may write a suppression and emit a never-billed
		// auto-reply, then the MO is still delivered below. A disabled number is out of service (no MT is
		// sent from it), so a STOP to it is moot and an auto-reply "from" it would be wrong — skip. A
		// transient failure returns (the MO is reprocessed; every effect is idempotent).
		if r.deps.Stop != nil && res.inboundNumberID != nil && res.reason != cp.UnroutedNumberDisabled {
			if err := r.deps.Stop.Detect(ctx, StopInput{
				MO: mo, InboundNumber: dest, From: normalizeAddr(mo.From), Body: body,
				InboundNumberID: *res.inboundNumberID, Country: res.country,
				AccountID: res.accountID, CustomerID: res.customerID,
			}); err != nil {
				return err
			}
		}

		// Count this MO into its source's velocity (step-066), best-effort: a recording failure is
		// logged (ids only) and never interrupts delivery — velocity is fail-open (§1.5).
		if r.deps.Velocity != nil {
			if err := r.deps.Velocity.RecordSource(ctx, normalizeAddr(mo.From)); err != nil {
				r.deps.Logger.WarnContext(ctx, "modlrrouter: MO velocity record failed, continuing", "err", err)
			}
		}

		if !res.routed {
			return r.recordUnrouted(ctx, span, mo, dest, res.reason, res.inboundNumberID)
		}

		// Derive the ids deterministically from the MO so a redelivered mo.inbound yields the SAME
		// message_id — the routed duplicate then collapses downstream by message_id, rather than a fresh
		// random id per attempt letting a duplicate reach the account (at-least-once idempotence).
		messageID := moMessageID(mo, body)
		routed := pipeline.MORouted{
			MessageID:       messageID,
			TraceID:         moTraceID(messageID),
			AccountID:       res.accountID,
			CustomerID:      res.customerID,
			InboundNumberID: derefID(res.inboundNumberID),
			ConnectorID:     mo.ConnectorID,
			From:            normalizeAddr(mo.From),
			To:              dest,
			Body:            mo.Body,
			DataCoding:      mo.DataCoding,
			Encoding:        encodingOf(mo.DataCoding),
			ESMClass:        mo.ESMClass,
			ReceivedAt:      mo.ReceivedAt,
		}
		out, err := pipeline.EncodeMORouted(routed)
		if err != nil {
			return fmt.Errorf("modlrrouter: encode mo.routed: %w", err)
		}
		if err := r.deps.Producer.Produce(ctx, out); err != nil {
			return fmt.Errorf("modlrrouter: publish mo.routed for %s: %w", routed.MessageID, err)
		}
		span.SetAttributes(attribute.Bool("mo.routed", true), attribute.String("account_id", res.accountID.String()))
		return nil
	}
}

// recordUnrouted files an MO that resolved to no account: it persists it for the operator, counts it,
// and logs it (ids + reason, never the body). A persistence failure is transient (return err → the
// record is reprocessed), so an unrouted MO is never lost.
func (r *MORouter) recordUnrouted(ctx context.Context, span trace.Span, mo pipeline.MOInbound, dest string, reason cp.UnroutedReason, inboundNumberID *uuid.UUID) error {
	connectorID := mo.ConnectorID
	if _, err := r.deps.Unrouted.Create(ctx, cp.NewUnroutedMO{
		ReceivedAt:      mo.ReceivedAt,
		ConnectorID:     &connectorID,
		InboundNumberID: inboundNumberID,
		SourceAddr:      normalizeAddr(mo.From),
		DestAddr:        dest,
		SegmentCount:    1,
		Encoding:        encodingOf(mo.DataCoding),
		Reason:          reason,
	}); err != nil {
		return fmt.Errorf("modlrrouter: record unrouted mo: %w", err)
	}
	r.deps.Metric.Inc(connectorID.String(), string(reason))
	span.SetAttributes(attribute.Bool("mo.routed", false), attribute.String("mo.unrouted_reason", string(reason)))
	r.deps.Logger.WarnContext(ctx, "modlrrouter: mo unrouted",
		"connector_id", connectorID, "dest", dest, "reason", reason)
	return nil
}

// The fixed UUIDv5 namespaces for MO ids. Deriving the ids (rather than uuid.New) makes routing
// idempotent under at-least-once redelivery of the same mo.inbound record.
var (
	moIDNamespace    = uuid.MustParse("7c3a1f9e-0b2d-5a4c-9e10-000000000045")
	moTraceNamespace = uuid.MustParse("7c3a1f9e-0b2d-5a4c-9e10-000000000046")
)

// moMessageID derives a stable message id from the MO's identity (connector, from, to, receive time)
// and a one-way hash of its body, so the same mo.inbound record always maps to the same id.
func moMessageID(mo pipeline.MOInbound, body []byte) uuid.UUID {
	sum := sha256.Sum256(body)
	key := fmt.Sprintf("%s|%s|%s|%d|%x", mo.ConnectorID, mo.From, mo.To, mo.ReceivedAt.UnixNano(), sum[:16])
	return uuid.NewSHA1(moIDNamespace, []byte(key))
}

// moTraceID derives a stable trace id from the message id.
func moTraceID(messageID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(moTraceNamespace, messageID[:])
}

// encodingOf maps an SMPP data_coding to the CDR encoding vocabulary.
func encodingOf(dataCoding uint8) string {
	switch dataCoding {
	case smpp.DataCodingUCS2:
		return "ucs2"
	case smpp.DataCodingBinary:
		return "binary"
	default:
		return "gsm7"
	}
}

// derefID returns the pointed id, or the nil UUID when the pointer is nil (a routed MO always has a
// number, so this is defensive).
func derefID(id *uuid.UUID) uuid.UUID {
	if id == nil {
		return uuid.Nil
	}
	return *id
}
