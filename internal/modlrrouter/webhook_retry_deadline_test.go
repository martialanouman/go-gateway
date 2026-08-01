package modlrrouter_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// TestSinkStampsAnAbsoluteDeadline proves the backoff is written as an ABSOLUTE instant at defer time,
// not left as a duration to be re-applied later. The distinction is what keeps the drain's throughput off
// the floor: with a duration, an event that already waited in the queue would sleep its full backoff
// again on top of that wait.
func TestSinkStampsAnAbsoluteDeadline(t *testing.T) {
	producer := &capturingProducer{}
	sink := modlrrouter.NewWebhookRetrySink(producer)

	before := time.Now()
	err := sink.Defer(context.Background(),
		cp.Webhook{ID: uuid.New(), AccountID: uuid.New(), EventType: cp.WebhookEventMO},
		webhook.Event{ID: "ev-1", Payload: []byte(`{}`)}, 1, before, "http status 503")
	if err != nil {
		t.Fatalf("Defer: %v", err)
	}

	var got struct {
		NotBefore time.Time `json:"not_before"`
	}
	if err := json.Unmarshal(producer.records[0].Value, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.NotBefore.IsZero() {
		t.Fatal("no not_before stamped — the runner would re-apply the full backoff on every pass")
	}
	if !got.NotBefore.After(before) {
		t.Errorf("not_before = %v, want an instant after the defer at %v", got.NotBefore, before)
	}
}

// deadlineRecord builds a webhook.retry record carrying an explicit not_before.
func deadlineRecord(t *testing.T, eventID string, attempt int, notBefore time.Time) kafka.Record {
	t.Helper()
	value, err := json.Marshal(map[string]any{
		"webhook_id": uuid.New().String(), "account_id": uuid.New().String(),
		"event_type": cp.WebhookEventMO, "event_id": eventID,
		"payload": json.RawMessage(`{"a":1}`), "attempt": attempt,
		"first_attempt_at": time.Now().Add(-time.Hour), "not_before": notBefore,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return kafka.Record{Topic: kafka.TopicWebhookRetry, Key: []byte(eventID), Value: value}
}

// TestRunnerDoesNotWaitForAnAlreadyDueEvent is the throughput fix. An event whose deadline has passed —
// because it sat in the queue behind others — must be attempted IMMEDIATELY. Sleeping again would cap the
// drain at roughly one event per backoff (about 2/min), orders of magnitude below the return-path volume,
// and would let a dead account's queue starve every other account behind it.
func TestRunnerDoesNotWaitForAnAlreadyDueEvent(t *testing.T) {
	var waited time.Duration
	pace := func(_ context.Context, d time.Duration) error { waited = d; return nil }

	getter := &fakeWebhookGetter{wh: cp.Webhook{ID: uuid.New(), URL: "https://x.test", Status: cp.WebhookActive}, found: true}
	sender := &fakeRetrySender{}
	runner := modlrrouter.NewWebhookRetryRunner(getter, sender, modlrrouter.WithRetryPace(pace))

	// Deadline 5 minutes in the past: the event waited its backoff already.
	rec := deadlineRecord(t, "ev-due", 4, time.Now().Add(-5*time.Minute))
	if err := runner.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if waited > 0 {
		t.Errorf("slept %v for an event already due — this is what caps the drain at ~2 events/min", waited)
	}
	if len(sender.calls) != 1 {
		t.Fatalf("attempted %d deliveries, want 1", len(sender.calls))
	}
}

// TestRunnerWaitsOnlyTheRemainingTime proves an event that is NOT yet due waits the remaining time only,
// never the full backoff again.
func TestRunnerWaitsOnlyTheRemainingTime(t *testing.T) {
	var waited time.Duration
	pace := func(_ context.Context, d time.Duration) error { waited = d; return nil }

	getter := &fakeWebhookGetter{wh: cp.Webhook{ID: uuid.New(), URL: "https://x.test", Status: cp.WebhookActive}, found: true}
	runner := modlrrouter.NewWebhookRetryRunner(getter, &fakeRetrySender{}, modlrrouter.WithRetryPace(pace))

	// Attempt 4 would be a 4-minute backoff; only 30s remain on the deadline.
	rec := deadlineRecord(t, "ev-soon", 4, time.Now().Add(30*time.Second))
	if err := runner.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if waited <= 0 {
		t.Fatal("did not wait for an event that is not yet due")
	}
	if waited > time.Minute {
		t.Errorf("waited %v, want only the ~30s remaining — the backoff must not restart", waited)
	}
}

// TestRunnerFallsBackToTheAttemptBackoff proves a record written before not_before existed still paces
// sensibly rather than being hammered immediately.
func TestRunnerFallsBackToTheAttemptBackoff(t *testing.T) {
	var waited time.Duration
	pace := func(_ context.Context, d time.Duration) error { waited = d; return nil }

	getter := &fakeWebhookGetter{wh: cp.Webhook{ID: uuid.New(), URL: "https://x.test", Status: cp.WebhookActive}, found: true}
	runner := modlrrouter.NewWebhookRetryRunner(getter, &fakeRetrySender{}, modlrrouter.WithRetryPace(pace))

	// A legacy record: no not_before at all.
	if err := runner.Handle(context.Background(), retryRecord(t, uuid.New(), "ev-legacy", 1, time.Now())); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if waited <= 0 {
		t.Error("a record without not_before was retried immediately — it must still be paced")
	}
}

// countingRetryMetric records what the runner reported.
type countingRetryMetric struct {
	handled map[string]int
	ages    []time.Duration
}

func (m *countingRetryMetric) Handled(outcome string) {
	if m.handled == nil {
		m.handled = map[string]int{}
	}
	m.handled[outcome]++
}
func (m *countingRetryMetric) Age(d time.Duration) { m.ages = append(m.ages, d) }

// TestRunnerReportsAgeAndOutcome proves the drain is observable. Without it there is no signal that a
// durably-unreachable account is filling the topic, and the first symptom would be a growing dead-letter —
// long after an operator could have acted.
func TestRunnerReportsAgeAndOutcome(t *testing.T) {
	metric := &countingRetryMetric{}
	getter := &fakeWebhookGetter{wh: cp.Webhook{ID: uuid.New(), URL: "https://x.test", Status: cp.WebhookActive}, found: true}
	runner := modlrrouter.NewWebhookRetryRunner(getter, &fakeRetrySender{},
		modlrrouter.WithRetryPace(noPace), modlrrouter.WithRetryMetric(metric))

	if err := runner.Handle(context.Background(), deadlineRecord(t, "ev-m", 2, time.Now().Add(-time.Minute))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if metric.handled["retried"] != 1 {
		t.Errorf("handled = %v, want one \"retried\"", metric.handled)
	}
	if len(metric.ages) != 1 || metric.ages[0] <= 0 {
		t.Errorf("ages = %v, want one positive age (time since the first attempt)", metric.ages)
	}

	// A webhook that no longer resolves is reported distinctly, so a rising drop rate is visible.
	dropRunner := modlrrouter.NewWebhookRetryRunner(&fakeWebhookGetter{found: false}, &fakeRetrySender{},
		modlrrouter.WithRetryPace(noPace), modlrrouter.WithRetryMetric(metric))
	if err := dropRunner.Handle(context.Background(), deadlineRecord(t, "ev-d", 1, time.Now())); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if metric.handled["dropped"] != 1 {
		t.Errorf("handled = %v, want one \"dropped\"", metric.handled)
	}
}
