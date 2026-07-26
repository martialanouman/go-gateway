package modlrrouter

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// LiveBind is one of an account's live binds, as SessionRegistry.Lookup reports it: the owning pod and
// the bind id. The bind role is not carried — a transmitter is discovered only when Deliver refuses it
// (FailedPrecondition), which the round-robin skips.
type LiveBind struct {
	PodID  string
	BindID string
}

// SessionLookup resolves an account's live binds via the SessionRegistry.
type SessionLookup interface {
	Lookup(ctx context.Context, accountID uuid.UUID) ([]LiveBind, error)
}

// PodDeliverer pushes an encoded deliver_sm to a bind on a pod, returning the gRPC status error the
// round-robin classifies. The concrete implementation dials the owning pod (a cached connection,
// address resolved from pod_id) and calls SessionRegistry.Deliver.
type PodDeliverer interface {
	Deliver(ctx context.Context, podID, bindID string, pdu []byte) error
}

// WebhookResolver fetches an account's webhook for an event type. *postgres.WebhookRepo satisfies it.
type WebhookResolver interface {
	Get(ctx context.Context, accountID uuid.UUID, eventType cp.WebhookEventType) (cp.Webhook, bool, error)
}

// WebhookSender delivers a webhook event. *webhook.Sender satisfies it.
type WebhookSender interface {
	Send(ctx context.Context, wh cp.Webhook, ev webhook.Event) error
}

// DeliveryMetric counts undeliverable events, labelled by event type and reason.
type DeliveryMetric interface {
	UndeliveredInc(eventType, reason string)
}

type noopDeliveryMetric struct{}

func (noopDeliveryMetric) UndeliveredInc(string, string) {}

// Delivery is one return-path event to hand to an account, prepared for both channels: PDU is the
// deliver_sm for a live bind, WebhookEvent is the payload for the webhook, and DeadLetter is the record
// parked verbatim when neither channel can deliver.
type Delivery struct {
	AccountID    uuid.UUID
	EventType    cp.WebhookEventType
	PDU          []byte
	WebhookEvent webhook.Event
	DeadLetter   kafka.Record
}

// Deliverer routes a resolved MO/DLR to its account: a live bind first (round-robin over the account's
// binds, skipping transmitters and dead binds), then the account's active webhook, then — only when no
// channel can deliver — a durable dead-letter so the event is never lost. A webhook that itself fails
// is the webhook sender's own dead-letter concern (step-047), never parked twice here.
type Deliverer struct {
	lookup   SessionLookup
	pods     PodDeliverer
	webhooks WebhookResolver
	sender   WebhookSender
	producer Producer
	metric   DeliveryMetric
	logger   *slog.Logger

	// rr rotates the starting bind across deliveries so an account's return traffic spreads over its
	// live binds instead of always hammering the first. Shared by both router goroutines, hence atomic.
	rr atomic.Uint64
}

// DelivererDeps are the deliverer's collaborators.
type DelivererDeps struct {
	Lookup   SessionLookup
	Pods     PodDeliverer
	Webhooks WebhookResolver
	Sender   WebhookSender
	Producer Producer
	Metric   DeliveryMetric
	Logger   *slog.Logger
}

// NewDeliverer builds a deliverer. A nil metric defaults to a no-op, a nil logger to slog.Default.
func NewDeliverer(deps DelivererDeps) *Deliverer {
	if deps.Metric == nil {
		deps.Metric = noopDeliveryMetric{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &Deliverer{
		lookup:   deps.Lookup,
		pods:     deps.Pods,
		webhooks: deps.Webhooks,
		sender:   deps.Sender,
		producer: deps.Producer,
		metric:   deps.Metric,
		logger:   deps.Logger,
	}
}

// Deliver hands d to its account. It returns nil once the event reaches a terminal state (delivered to
// a bind, handed to the webhook sender, or dead-lettered). It returns an error only when the work could
// not be completed and must be reprocessed: a lookup/Postgres failure, a malformed PDU (our bug), or a
// failed park. The message body never appears in a log along the way (invariant a).
func (dv *Deliverer) Deliver(ctx context.Context, d Delivery) error {
	delivered, hadBinds, err := dv.tryBinds(ctx, d.AccountID, d.PDU)
	if err != nil {
		return err
	}
	if delivered {
		return nil
	}

	wh, found, err := dv.webhooks.Get(ctx, d.AccountID, d.EventType)
	if err != nil {
		return fmt.Errorf("modlrrouter: resolve webhook for %s: %w", d.AccountID, err)
	}
	if found && wh.Status == cp.WebhookActive {
		// The webhook sender owns retries and its own dead-letter; we never park a webhook event here.
		return dv.sender.Send(ctx, wh, d.WebhookEvent)
	}

	// No live bind delivered and no active webhook: dead-letter so the event is not lost.
	reason := "no_bind"
	if hadBinds {
		reason = "bind_exhausted"
	}
	dv.metric.UndeliveredInc(string(d.EventType), reason)
	dv.logger.WarnContext(ctx, "modlrrouter: no delivery path, dead-lettering",
		"account_id", d.AccountID, "event_type", d.EventType, "reason", reason)
	if err := dv.producer.Produce(ctx, d.DeadLetter); err != nil {
		return fmt.Errorf("modlrrouter: dead-letter %s: %w", d.EventType, err)
	}
	return nil
}

// DeadLetterRaw parks a record that could not even be prepared for delivery — a deterministic failure
// on our side (a PDU or webhook payload we could not encode from our own envelope). Retrying would fail
// identically, and the resolved MO/DLR must not be silently lost, so it is parked verbatim for the
// operator. It counts the drop and returns an error only if the park itself fails (worth reprocessing).
func (dv *Deliverer) DeadLetterRaw(ctx context.Context, rec kafka.Record, eventType, reason string) error {
	dv.metric.UndeliveredInc(eventType, reason)
	dv.logger.ErrorContext(ctx, "modlrrouter: parking undeliverable record", "topic", rec.Topic, "reason", reason)
	if err := dv.producer.Produce(ctx, rec); err != nil {
		return fmt.Errorf("modlrrouter: dead-letter %s (%s): %w", eventType, reason, err)
	}
	return nil
}

// tryBinds attempts delivery to each of the account's live binds in round-robin order (a rotating
// start offset), returning as soon as one accepts the PDU. hadBinds reports whether the account had any
// live bind at all (to distinguish "no_bind" from "bind_exhausted" downstream). It returns an error
// only for a transient failure worth reprocessing (the Lookup itself). A malformed PDU (InvalidArgument)
// is our own bug: no bind will accept it and a redelivery cannot fix it, so we log it (ids only) and
// stop the walk WITHOUT an error — the caller falls back to the webhook (built independently of the
// PDU), else the dead-letter. Never wedge the consumer on a deterministic fault. Every other per-bind
// failure (a transmitter, a dead or moved bind, a transport error) just moves to the next bind.
func (dv *Deliverer) tryBinds(ctx context.Context, accountID uuid.UUID, pdu []byte) (delivered, hadBinds bool, err error) {
	binds, err := dv.lookup.Lookup(ctx, accountID)
	if err != nil {
		return false, false, fmt.Errorf("modlrrouter: lookup binds for %s: %w", accountID, err)
	}
	if len(binds) == 0 {
		return false, false, nil
	}
	//nolint:gosec // modulo len(binds) is always in [0,len), so the int conversion never overflows.
	start := int(dv.rr.Add(1) % uint64(len(binds)))
	for i := range binds {
		b := binds[(start+i)%len(binds)]
		derr := dv.pods.Deliver(ctx, b.PodID, b.BindID, pdu)
		switch status.Code(derr) {
		case codes.OK:
			return true, true, nil
		case codes.InvalidArgument:
			// The deliver_sm we encoded is malformed — a bug on our side, identical for every bind and
			// every redelivery. Record it (ids only, never the body) and fall back rather than crash-loop.
			dv.logger.ErrorContext(ctx, "modlrrouter: malformed deliver_sm rejected by pod, falling back",
				"account_id", accountID, "bind_id", b.BindID, "err", derr)
			return false, true, nil
		default:
			// FailedPrecondition (transmitter), NotFound / Unavailable (bind gone), or a transport error:
			// try the next bind.
			continue
		}
	}
	return false, true, nil
}
