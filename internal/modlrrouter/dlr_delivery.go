package modlrrouter

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// DLRDeliveryRouter consumes dlr.events on its OWN consumer group — independent of the CDR correlator
// (step-044) — re-resolves each receipt to its account via the dlrmap, and hands it to the deliverer.
// The two dlr.events consumers (CDR and delivery) fail and replay independently by design.
type DLRDeliveryRouter struct {
	consumer    Consumer
	resolver    Resolver
	deliverer   *Deliverer
	mappingMiss UnmappedCounter
	tracer      trace.Tracer
	logger      *slog.Logger
}

// DLRDeliveryDeps are the DLR delivery router's collaborators. MappingMiss is a counter distinct from
// step-044's dlr_unmapped_total: a delivery-side miss (the mapping's TTL elapsed before delivery) is a
// different signal from a CDR-side miss.
type DLRDeliveryDeps struct {
	Consumer    Consumer
	Resolver    Resolver
	Deliverer   *Deliverer
	MappingMiss UnmappedCounter
	Tracer      trace.Tracer
	Logger      *slog.Logger
}

// NewDLRDeliveryRouter builds the DLR delivery router. A nil logger defaults to slog.Default, a nil
// mapping-miss counter to a no-op.
func NewDLRDeliveryRouter(deps DLRDeliveryDeps) *DLRDeliveryRouter {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.MappingMiss == nil {
		deps.MappingMiss = noopCounter{}
	}
	if deps.Tracer == nil {
		deps.Tracer = noop.NewTracerProvider().Tracer("")
	}
	return &DLRDeliveryRouter{
		consumer: deps.Consumer, resolver: deps.Resolver, deliverer: deps.Deliverer,
		mappingMiss: deps.MappingMiss, tracer: deps.Tracer, logger: deps.Logger,
	}
}

// Run consumes dlr.events until ctx is cancelled.
func (r *DLRDeliveryRouter) Run(ctx context.Context) error {
	return r.consumer.Run(ctx, r.handler())
}

func (r *DLRDeliveryRouter) handler() kafka.Handler {
	return func(ctx context.Context, rec kafka.Record) error {
		ctx, span := r.tracer.Start(ctx, "dlr.deliver")
		defer span.End()

		dlr, err := pipeline.DecodeDLR(rec)
		if err != nil {
			observability.RecordSpanError(span, err)
			r.logger.ErrorContext(ctx, "modlrrouter: undecodable dlr.events record, skipping", "err", err)
			return nil
		}
		m, found, err := r.resolver.Get(ctx, dlr.ConnectorID, dlr.SMSCMessageID)
		if err != nil {
			// A Redis infrastructure error is transient: reprocess once it recovers.
			return fmt.Errorf("modlrrouter: resolve dlr %s/%s: %w", dlr.ConnectorID, dlr.SMSCMessageID, err)
		}
		if !found {
			// The mapping's TTL elapsed (or an unknown smsc id): the receipt cannot be routed to an
			// account. Count and log — never silently — then commit (redelivery would not resolve it).
			r.mappingMiss.Inc()
			r.logger.WarnContext(ctx, "modlrrouter: dlr mapping miss on delivery, counted",
				"connector_id", dlr.ConnectorID, "smsc_message_id", dlr.SMSCMessageID)
			return nil
		}

		deadLetter := kafka.Record{Topic: kafka.TopicDLRDeadLetter, Key: rec.Key, Value: rec.Value}
		pdu, err := dlrDeliverSM(m, dlr)
		if err != nil {
			observability.RecordSpanError(span, err)
			r.logger.ErrorContext(ctx, "modlrrouter: encode dlr deliver_sm, dead-lettering", "message_id", m.MessageID, "err", err)
			return r.deliverer.DeadLetterRaw(ctx, deadLetter, string(cp.WebhookEventDLR), "encode_error")
		}
		evID, payload, err := dlrWebhookBody(m, dlr)
		if err != nil {
			observability.RecordSpanError(span, err)
			r.logger.ErrorContext(ctx, "modlrrouter: build dlr webhook payload, dead-lettering", "message_id", m.MessageID, "err", err)
			return r.deliverer.DeadLetterRaw(ctx, deadLetter, string(cp.WebhookEventDLR), "encode_error")
		}
		return r.deliverer.Deliver(ctx, Delivery{
			AccountID:    m.AccountID,
			EventType:    cp.WebhookEventDLR,
			PDU:          pdu,
			WebhookEvent: webhook.Event{ID: evID, Payload: payload},
			DeadLetter:   deadLetter,
		})
	}
}
