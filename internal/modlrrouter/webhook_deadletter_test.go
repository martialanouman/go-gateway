package modlrrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

type recProducer struct{ recs []kafka.Record }

func (p *recProducer) Produce(_ context.Context, rec kafka.Record) error {
	p.recs = append(p.recs, rec)
	return nil
}

// TestWebhookDeadLetterParksWithoutSecret parks an abandoned webhook event on webhook.dead-letter,
// keyed by event id, carrying the payload and reason — and never the signing secret.
func TestWebhookDeadLetterParksWithoutSecret(t *testing.T) {
	prod := &recProducer{}
	sink := NewWebhookDeadLetterSink(prod)

	wh := cp.Webhook{
		ID: uuid.New(), AccountID: uuid.New(), EventType: cp.WebhookEventMO,
		URL: "https://acct.test/hook", Secret: "whsec_super_secret",
	}
	ev := webhook.Event{ID: "ev-42", Payload: []byte(`{"from":"22507000001"}`)}

	if err := sink.Park(context.Background(), wh, ev, "http status 500"); err != nil {
		t.Fatalf("Park: %v", err)
	}
	if len(prod.recs) != 1 {
		t.Fatalf("parked %d records, want 1", len(prod.recs))
	}
	rec := prod.recs[0]
	if rec.Topic != kafka.TopicWebhookDeadLetter {
		t.Errorf("topic = %q, want %q", rec.Topic, kafka.TopicWebhookDeadLetter)
	}
	if string(rec.Key) != "ev-42" {
		t.Errorf("key = %q, want ev-42", rec.Key)
	}
	if bytes.Contains(rec.Value, []byte("whsec_super_secret")) {
		t.Fatal("dead-letter record must not carry the webhook secret")
	}

	var got webhookDeadLetter
	if err := json.Unmarshal(rec.Value, &got); err != nil {
		t.Fatalf("unmarshal parked value: %v", err)
	}
	if got.EventID != "ev-42" || got.URL != wh.URL || got.Reason != "http status 500" {
		t.Errorf("parked = %+v, want event ev-42 / url %q / reason set", got, wh.URL)
	}
	if string(got.Payload) != string(ev.Payload) {
		t.Errorf("parked payload = %q, want %q", got.Payload, ev.Payload)
	}
}
