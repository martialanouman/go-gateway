package modlrrouter

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// MODeliveryRouter consumes mo.routed (the resolved MO from step-045) and hands each one to the
// deliverer: a live bind, else the account's webhook, else the dead-letter.
type MODeliveryRouter struct {
	consumer  Consumer
	deliverer *Deliverer
	tracer    trace.Tracer
	logger    *slog.Logger
}

// MODeliveryDeps are the MO delivery router's collaborators.
type MODeliveryDeps struct {
	Consumer  Consumer
	Deliverer *Deliverer
	Tracer    trace.Tracer
	Logger    *slog.Logger
}

// NewMODeliveryRouter builds the MO delivery router. A nil logger defaults to slog.Default.
func NewMODeliveryRouter(deps MODeliveryDeps) *MODeliveryRouter {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Tracer == nil {
		deps.Tracer = noop.NewTracerProvider().Tracer("")
	}
	return &MODeliveryRouter{consumer: deps.Consumer, deliverer: deps.Deliverer, tracer: deps.Tracer, logger: deps.Logger}
}

// Run consumes mo.routed until ctx is cancelled.
func (r *MODeliveryRouter) Run(ctx context.Context) error {
	return r.consumer.Run(ctx, r.handler())
}

func (r *MODeliveryRouter) handler() kafka.Handler {
	return func(ctx context.Context, rec kafka.Record) error {
		ctx, span := r.tracer.Start(ctx, "mo.deliver")
		defer span.End()

		mo, err := pipeline.DecodeMORouted(rec)
		if err != nil {
			observability.RecordSpanError(span, err)
			r.logger.ErrorContext(ctx, "modlrrouter: undecodable mo.routed record, skipping", "err", err)
			return nil
		}
		deadLetter := kafka.Record{Topic: kafka.TopicMODeadLetter, Key: rec.Key, Value: rec.Value}
		pdu, err := moDeliverSM(mo)
		if err != nil {
			// Encoding the PDU from our own envelope failing is a deterministic bug: retrying is futile,
			// but the resolved MO must not be lost — park it verbatim. Ids only, never the body.
			observability.RecordSpanError(span, err)
			r.logger.ErrorContext(ctx, "modlrrouter: encode mo deliver_sm, dead-lettering", "message_id", mo.MessageID, "err", err)
			return r.deliverer.DeadLetterRaw(ctx, deadLetter, string(cp.WebhookEventMO), "encode_error")
		}
		evID, payload, err := moWebhookBody(mo)
		if err != nil {
			observability.RecordSpanError(span, err)
			r.logger.ErrorContext(ctx, "modlrrouter: build mo webhook payload, dead-lettering", "message_id", mo.MessageID, "err", err)
			return r.deliverer.DeadLetterRaw(ctx, deadLetter, string(cp.WebhookEventMO), "encode_error")
		}
		return r.deliverer.Deliver(ctx, Delivery{
			AccountID:    mo.AccountID,
			EventType:    cp.WebhookEventMO,
			PDU:          pdu,
			WebhookEvent: webhook.Event{ID: evID, Payload: payload},
			DeadLetter:   deadLetter,
		})
	}
}
