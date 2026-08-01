package modlrrouter_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

type retryAttempt struct {
	whURL   string
	eventID string
	attempt int
	firstAt time.Time
}

type fakeRetrySender struct {
	calls []retryAttempt
	err   error
}

func (f *fakeRetrySender) Retry(_ context.Context, wh cp.Webhook, ev webhook.Event, attempt int, firstAt time.Time) error {
	f.calls = append(f.calls, retryAttempt{whURL: wh.URL, eventID: ev.ID, attempt: attempt, firstAt: firstAt})
	return f.err
}

type fakeWebhookGetter struct {
	wh    cp.Webhook
	found bool
	err   error
}

func (f *fakeWebhookGetter) Get(context.Context, uuid.UUID, cp.WebhookEventType) (cp.Webhook, bool, error) {
	return f.wh, f.found, f.err
}

// retryRecord builds a webhook.retry record as the sink writes it.
func retryRecord(t *testing.T, accountID uuid.UUID, eventID string, attempt int, firstAt time.Time) kafka.Record {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"webhook_id": uuid.New().String(), "account_id": accountID.String(),
		"event_type": cp.WebhookEventMO, "event_id": eventID,
		"payload": json.RawMessage(`{"a":1}`), "attempt": attempt, "first_attempt_at": firstAt,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafka.Record{Topic: kafka.TopicWebhookRetry, Key: []byte(eventID), Value: value}
}

// TestRetryRunnerReResolvesTheWebhook proves the consumer fetches the webhook — and therefore its signing
// secret — from the control plane rather than from the queued record. The record deliberately omits the
// secret, and re-resolving also means a rotated secret or an edited URL takes effect on the next pass.
func TestRetryRunnerReResolvesTheWebhook(t *testing.T) {
	accountID := uuid.New()
	getter := &fakeWebhookGetter{
		wh:    cp.Webhook{ID: uuid.New(), AccountID: accountID, URL: "https://current.test/hook", Secret: "rotated", Status: cp.WebhookActive},
		found: true,
	}
	sender := &fakeRetrySender{}
	runner := modlrrouter.NewWebhookRetryRunner(getter, sender, modlrrouter.WithRetryPace(noPace))

	firstAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	if err := runner.Handle(context.Background(), retryRecord(t, accountID, "ev-1", 2, firstAt)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(sender.calls) != 1 {
		t.Fatalf("retried %d events, want 1", len(sender.calls))
	}
	got := sender.calls[0]
	if got.whURL != "https://current.test/hook" {
		t.Errorf("URL = %q, want the freshly resolved one", got.whURL)
	}
	if got.attempt != 2 || !got.firstAt.Equal(firstAt) {
		t.Errorf("state = (attempt %d, firstAt %v), want (2, %v)", got.attempt, got.firstAt, firstAt)
	}
}

// TestRetryRunnerPacesBeforeRetrying proves the consumer waits before re-attempting. Without it the runner
// would spin the retry topic at full speed against an endpoint that is already failing — hammering a
// struggling customer and burning the attempt budget in milliseconds.
func TestRetryRunnerPacesBeforeRetrying(t *testing.T) {
	var waited time.Duration
	pace := func(_ context.Context, d time.Duration) error { waited = d; return nil }

	getter := &fakeWebhookGetter{wh: cp.Webhook{ID: uuid.New(), URL: "https://x.test", Status: cp.WebhookActive}, found: true}
	runner := modlrrouter.NewWebhookRetryRunner(getter, &fakeRetrySender{}, modlrrouter.WithRetryPace(pace))

	if err := runner.Handle(context.Background(), retryRecord(t, uuid.New(), "ev-2", 1, time.Now())); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if waited <= 0 {
		t.Fatal("the runner retried immediately — it must pace, or it hammers a failing endpoint")
	}
}

// TestRetryRunnerBacksOffFurtherEachPass proves the wait grows with the attempt count, so a persistently
// failing endpoint is probed ever less often instead of at a constant rate.
func TestRetryRunnerBacksOffFurtherEachPass(t *testing.T) {
	var waits []time.Duration
	pace := func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }

	getter := &fakeWebhookGetter{wh: cp.Webhook{ID: uuid.New(), URL: "https://x.test", Status: cp.WebhookActive}, found: true}
	runner := modlrrouter.NewWebhookRetryRunner(getter, &fakeRetrySender{}, modlrrouter.WithRetryPace(pace))

	for _, attempt := range []int{1, 3} {
		if err := runner.Handle(context.Background(), retryRecord(t, uuid.New(), "ev", attempt, time.Now())); err != nil {
			t.Fatalf("Handle(attempt=%d): %v", attempt, err)
		}
	}
	if len(waits) != 2 {
		t.Fatalf("recorded %d waits, want 2", len(waits))
	}
	if waits[1] <= waits[0] {
		t.Errorf("waits = %v then %v; the backoff must grow with the attempt count", waits[0], waits[1])
	}
}

// TestRetryRunnerDropsAnUnresolvableWebhook proves a deleted or disabled webhook ends the cycle instead of
// erroring forever. Returning an error would block the partition on a record that can never succeed — the
// very head-of-line blocking this topic exists to remove.
func TestRetryRunnerDropsAnUnresolvableWebhook(t *testing.T) {
	sender := &fakeRetrySender{}
	runner := modlrrouter.NewWebhookRetryRunner(&fakeWebhookGetter{found: false}, sender, modlrrouter.WithRetryPace(noPace))

	if err := runner.Handle(context.Background(), retryRecord(t, uuid.New(), "ev-3", 1, time.Now())); err != nil {
		t.Fatalf("Handle = %v, want nil: an unresolvable webhook must not wedge the partition", err)
	}
	if len(sender.calls) != 0 {
		t.Errorf("attempted delivery to a webhook that no longer exists: %+v", sender.calls)
	}
}

// TestRetryRunnerSurfacesSenderFailure proves a failure that must be retried is propagated, leaving the
// offset uncommitted so the record is redelivered rather than lost.
func TestRetryRunnerSurfacesSenderFailure(t *testing.T) {
	getter := &fakeWebhookGetter{wh: cp.Webhook{ID: uuid.New(), URL: "https://x.test", Status: cp.WebhookActive}, found: true}
	sender := &fakeRetrySender{err: errors.New("kafka down")}
	runner := modlrrouter.NewWebhookRetryRunner(getter, sender, modlrrouter.WithRetryPace(noPace))

	if err := runner.Handle(context.Background(), retryRecord(t, uuid.New(), "ev-4", 1, time.Now())); err == nil {
		t.Fatal("Handle = nil although the sender could not complete — the record would be committed and lost")
	}
}

// TestRetryRunnerSkipsAMalformedRecord proves a corrupt record is dropped, not retried forever. It cannot
// be parsed, so redelivering it would block the partition permanently.
func TestRetryRunnerSkipsAMalformedRecord(t *testing.T) {
	sender := &fakeRetrySender{}
	runner := modlrrouter.NewWebhookRetryRunner(&fakeWebhookGetter{found: true}, sender, modlrrouter.WithRetryPace(noPace))

	rec := kafka.Record{Topic: kafka.TopicWebhookRetry, Key: []byte("ev-5"), Value: []byte("not json")}
	if err := runner.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle = %v, want nil: a corrupt record must be skipped, not block the partition", err)
	}
	if len(sender.calls) != 0 {
		t.Errorf("attempted delivery from a corrupt record: %+v", sender.calls)
	}
}

func noPace(context.Context, time.Duration) error { return nil }
