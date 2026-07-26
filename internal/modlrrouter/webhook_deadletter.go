package modlrrouter

import (
	"context"
	"encoding/json"
	"fmt"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// WebhookDeadLetterSink parks a webhook event whose delivery was abandoned (retries exhausted or a
// permanent rejection) onto a durable Kafka topic, so an operator can inspect and replay it. It
// satisfies webhook.DeadLetterSink and is wired into the webhook.Sender (step-048).
type WebhookDeadLetterSink struct {
	producer Producer
}

// NewWebhookDeadLetterSink builds the sink over the shared Kafka producer.
func NewWebhookDeadLetterSink(producer Producer) *WebhookDeadLetterSink {
	return &WebhookDeadLetterSink{producer: producer}
}

// webhookDeadLetter is the parked record's JSON value. The event payload legitimately carries the
// account's own body — it is durable data, never logged (invariant a). The webhook secret is NOT
// included: a park record is operator-visible and must not leak a signing key.
type webhookDeadLetter struct {
	WebhookID string              `json:"webhook_id"`
	AccountID string              `json:"account_id"`
	EventType cp.WebhookEventType `json:"event_type"`
	URL       string              `json:"url"`
	EventID   string              `json:"event_id"`
	Payload   json.RawMessage     `json:"payload"`
	Reason    string              `json:"reason"`
}

// Park serializes the abandoned event and produces it to webhook.dead-letter, keyed by the event id
// so an operator replaying a specific event lands its records on one partition.
func (s *WebhookDeadLetterSink) Park(ctx context.Context, wh cp.Webhook, ev webhook.Event, reason string) error {
	value, err := json.Marshal(webhookDeadLetter{
		WebhookID: wh.ID.String(),
		AccountID: wh.AccountID.String(),
		EventType: wh.EventType,
		URL:       wh.URL,
		EventID:   ev.ID,
		Payload:   ev.Payload,
		Reason:    reason,
	})
	if err != nil {
		return fmt.Errorf("modlrrouter: marshal webhook dead-letter %s: %w", ev.ID, err)
	}
	return s.producer.Produce(ctx, kafka.Record{
		Topic: kafka.TopicWebhookDeadLetter,
		Key:   []byte(ev.ID),
		Value: value,
	})
}
