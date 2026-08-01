package modlrrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// WebhookRetrySink queues a transiently-failed webhook event on a durable Kafka topic, to be attempted
// again later by a paced consumer rather than in band on the delivery goroutine (step-192). It satisfies
// webhook.RetrySink.
type WebhookRetrySink struct {
	producer Producer
}

// NewWebhookRetrySink builds the sink over the shared Kafka producer.
func NewWebhookRetrySink(producer Producer) *WebhookRetrySink {
	return &WebhookRetrySink{producer: producer}
}

// webhookRetry is the queued record's JSON value. The event payload legitimately carries the account's own
// body — durable data, never logged (invariant a). The webhook secret is NOT included: a queued record is
// operator-visible and outlives the request, so persisting the signing key would leak a credential able to
// forge signed deliveries. The consumer re-resolves the webhook, and its secret, from the control plane by
// webhook_id — which also means a rotated secret or an edited URL takes effect on the next retry.
type webhookRetry struct {
	WebhookID string              `json:"webhook_id"`
	AccountID string              `json:"account_id"`
	EventType cp.WebhookEventType `json:"event_type"`
	EventID   string              `json:"event_id"`
	Payload   json.RawMessage     `json:"payload"`
	// Attempt is how many attempts have been SPENT, so the next pass is Attempt+1.
	Attempt int `json:"attempt"`
	// FirstAttemptAt is when the event was FIRST tried, carried unchanged across passes: the maximum-age
	// bound is measured from it, and resetting it per pass would let a doomed event cycle forever.
	FirstAttemptAt time.Time `json:"first_attempt_at"`
	// NotBefore is the ABSOLUTE instant this event becomes due, stamped here at defer time rather than
	// left as a duration for the consumer to re-apply. That distinction sets the drain's throughput: with
	// a duration, an event that already waited its backoff sitting in the queue would sleep the whole
	// backoff AGAIN, capping the drain near one event per backoff and letting one dead account's queue
	// starve every account behind it. With an instant, a queued event arrives already due and is attempted
	// at once.
	NotBefore time.Time `json:"not_before"`
	// Reason is why the last attempt failed. Diagnostic only, never the payload.
	Reason string `json:"reason"`
}

// Defer serializes the event and produces it to webhook.retry, keyed by the event id so every pass of one
// event lands on the same partition and its retries stay ordered.
func (s *WebhookRetrySink) Defer(ctx context.Context, wh cp.Webhook, ev webhook.Event, attempt int, firstAt time.Time, reason string) error {
	value, err := json.Marshal(webhookRetry{
		WebhookID:      wh.ID.String(),
		AccountID:      wh.AccountID.String(),
		EventType:      wh.EventType,
		EventID:        ev.ID,
		Payload:        ev.Payload,
		Attempt:        attempt,
		FirstAttemptAt: firstAt,
		NotBefore:      time.Now().Add(paceFor(attempt)),
		Reason:         reason,
	})
	if err != nil {
		return fmt.Errorf("modlrrouter: marshal webhook retry %s: %w", ev.ID, err)
	}
	return s.producer.Produce(ctx, kafka.Record{
		Topic: kafka.TopicWebhookRetry,
		Key:   []byte(ev.ID),
		Value: value,
	})
}
