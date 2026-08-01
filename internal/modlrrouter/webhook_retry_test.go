package modlrrouter_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

type capturingProducer struct {
	records []kafka.Record
	err     error
}

func (p *capturingProducer) Produce(_ context.Context, r kafka.Record) error {
	if p.err != nil {
		return p.err
	}
	p.records = append(p.records, r)
	return nil
}

// TestWebhookRetrySinkNeverPersistsTheSecret is the security guard, mirroring the dead-letter sink's rule.
// A queued record is operator-visible and outlives the request, so writing the webhook's signing key into
// Kafka would leak a credential able to forge signed deliveries to that customer's endpoint.
func TestWebhookRetrySinkNeverPersistsTheSecret(t *testing.T) {
	producer := &capturingProducer{}
	sink := modlrrouter.NewWebhookRetrySink(producer)

	const secret = "super-secret-signing-key"
	wh := cp.Webhook{
		ID: uuid.New(), AccountID: uuid.New(), EventType: cp.WebhookEventMO,
		URL: "https://example.test/hook", Secret: secret, Status: cp.WebhookActive,
	}
	ev := webhook.Event{ID: "ev-1", Payload: []byte(`{"msg":"hello"}`)}

	if err := sink.Defer(context.Background(), wh, ev, 1, time.Now(), "http status 503"); err != nil {
		t.Fatalf("Defer: %v", err)
	}
	if len(producer.records) != 1 {
		t.Fatalf("produced %d records, want 1", len(producer.records))
	}
	if strings.Contains(string(producer.records[0].Value), secret) {
		t.Fatal("the webhook secret was written to webhook.retry — a leaked signing key lets anyone forge deliveries")
	}
}

// TestWebhookRetrySinkCarriesTheRetryState proves the record carries what a later pass needs: which
// webhook to re-resolve, the spent attempt count, and the ORIGINAL first-attempt instant that bounds the
// event's total lifetime. Losing firstAt would reset the age bound on every pass and let a doomed event
// cycle forever.
func TestWebhookRetrySinkCarriesTheRetryState(t *testing.T) {
	producer := &capturingProducer{}
	sink := modlrrouter.NewWebhookRetrySink(producer)

	webhookID, accountID := uuid.New(), uuid.New()
	firstAt := time.Now().Add(-12 * time.Minute).UTC().Truncate(time.Second)
	wh := cp.Webhook{
		ID: webhookID, AccountID: accountID, EventType: cp.WebhookEventDLR,
		URL: "https://example.test/hook", Secret: "s", Status: cp.WebhookActive,
	}
	ev := webhook.Event{ID: "ev-2", Payload: []byte(`{"a":1}`)}

	if err := sink.Defer(context.Background(), wh, ev, 3, firstAt, "transport error"); err != nil {
		t.Fatalf("Defer: %v", err)
	}

	rec := producer.records[0]
	if rec.Topic != kafka.TopicWebhookRetry {
		t.Errorf("topic = %q, want %q", rec.Topic, kafka.TopicWebhookRetry)
	}
	// Keyed by event id so every pass of one event lands on the same partition, keeping its retries ordered.
	if string(rec.Key) != ev.ID {
		t.Errorf("key = %q, want the event id %q", rec.Key, ev.ID)
	}

	var got struct {
		WebhookID string          `json:"webhook_id"`
		AccountID string          `json:"account_id"`
		EventID   string          `json:"event_id"`
		Payload   json.RawMessage `json:"payload"`
		Attempt   int             `json:"attempt"`
		FirstAt   time.Time       `json:"first_attempt_at"`
	}
	if err := json.Unmarshal(rec.Value, &got); err != nil {
		t.Fatalf("unmarshal record: %v", err)
	}
	if got.WebhookID != webhookID.String() {
		t.Errorf("webhook_id = %q, want %q — the consumer re-resolves the secret from it", got.WebhookID, webhookID)
	}
	if got.AccountID != accountID.String() {
		t.Errorf("account_id = %q, want %q", got.AccountID, accountID)
	}
	if got.EventID != ev.ID || string(got.Payload) != string(ev.Payload) {
		t.Errorf("event = %q/%s, want %q/%s", got.EventID, got.Payload, ev.ID, ev.Payload)
	}
	if got.Attempt != 3 {
		t.Errorf("attempt = %d, want 3", got.Attempt)
	}
	if !got.FirstAt.Equal(firstAt) {
		t.Errorf("first_attempt_at = %v, want the original %v — the age bound must not reset each pass", got.FirstAt, firstAt)
	}
}

// TestWebhookRetrySinkSurfacesProducerFailure proves a broker outage is reported, so the caller leaves its
// source offset uncommitted and the event is redelivered rather than silently lost.
func TestWebhookRetrySinkSurfacesProducerFailure(t *testing.T) {
	sink := modlrrouter.NewWebhookRetrySink(&capturingProducer{err: context.DeadlineExceeded})
	err := sink.Defer(context.Background(), cp.Webhook{ID: uuid.New(), AccountID: uuid.New()},
		webhook.Event{ID: "ev-3", Payload: []byte(`{}`)}, 1, time.Now(), "boom")
	if err == nil {
		t.Fatal("Defer = nil although the producer failed — the caller would commit and lose the event")
	}
}
