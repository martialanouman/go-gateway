package modlrrouter

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// Retry pacing. The wait grows with the attempt count so a persistently failing endpoint is probed ever
// less often, and is capped so a long-running retry never stalls the runner past a useful cadence.
const (
	retryPaceBase = 30 * time.Second
	retryPaceMax  = 10 * time.Minute
)

// WebhookGetter resolves an account's active webhook for an event type. *postgres.WebhookRepo satisfies
// it; declared consumer-side.
type WebhookGetter interface {
	Get(ctx context.Context, accountID uuid.UUID, eventType cp.WebhookEventType) (cp.Webhook, bool, error)
}

// RetrySender makes one further delivery attempt at a deferred event. *webhook.Sender satisfies it.
type RetrySender interface {
	Retry(ctx context.Context, wh cp.Webhook, ev webhook.Event, attempt int, firstAt time.Time) error
}

// RetryMetric observes the drain. Handled is labelled by outcome (a bounded label — "retried", "dropped"
// or "skipped", never an account or event id). Age is how long an event has been in retry, counted from
// its first attempt: a rising age is the signal that an account's endpoint is durably unreachable, and it
// shows up well before the dead-letter starts growing.
type RetryMetric interface {
	Handled(outcome string)
	Age(d time.Duration)
}

type nopRetryMetric struct{}

func (nopRetryMetric) Handled(string)    {}
func (nopRetryMetric) Age(time.Duration) {}

// WebhookRetryRunner drains webhook.retry: it re-resolves each queued event's webhook, waits until the
// event is due, and hands it back to the sender for one more attempt (step-192).
//
// It runs on its OWN consumer group and goroutine, which is the whole point: the delivery consumer must
// never wait on a slow endpoint.
//
// The wait is the time REMAINING until each event's stamped deadline, not a fresh backoff per record.
// That is what keeps the drain's throughput usable: waiting the full backoff again on an event that had
// already come due while queued would cap the whole drain near one event per backoff — orders of
// magnitude below return-path volume — and let one dead account's queue starve every account behind it.
// Records are still processed serially, so the drain remains a recovery path rather than a delivery one;
// a backlog of events that are all genuinely due now drains at Kafka speed.
type WebhookRetryRunner struct {
	webhooks WebhookGetter
	sender   RetrySender
	pace     func(ctx context.Context, d time.Duration) error
	metric   RetryMetric
	logger   *slog.Logger
}

// RetryRunnerOption configures a WebhookRetryRunner.
type RetryRunnerOption func(*WebhookRetryRunner)

// WithRetryPace overrides the wait applied before each re-attempt (a test passes a no-op).
func WithRetryPace(pace func(ctx context.Context, d time.Duration) error) RetryRunnerOption {
	return func(r *WebhookRetryRunner) {
		if pace != nil {
			r.pace = pace
		}
	}
}

// WithRetryMetric wires the drain observability.
func WithRetryMetric(m RetryMetric) RetryRunnerOption {
	return func(r *WebhookRetryRunner) {
		if m != nil {
			r.metric = m
		}
	}
}

// WithRetryRunnerLogger sets the logger (defaults to slog.Default).
func WithRetryRunnerLogger(l *slog.Logger) RetryRunnerOption {
	return func(r *WebhookRetryRunner) {
		if l != nil {
			r.logger = l
		}
	}
}

// NewWebhookRetryRunner builds the runner over the webhook store and the sender.
func NewWebhookRetryRunner(webhooks WebhookGetter, sender RetrySender, opts ...RetryRunnerOption) *WebhookRetryRunner {
	r := &WebhookRetryRunner{
		webhooks: webhooks, sender: sender, pace: sleepCtx,
		metric: nopRetryMetric{}, logger: slog.Default(),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// Handle processes one webhook.retry record. It returns an error ONLY when the work must be redelivered —
// the webhook store or the sender could not complete. Anything unrecoverable (a corrupt record, a webhook
// that no longer exists) is logged and skipped: returning an error there would block the partition
// forever on a record that can never succeed, which is the very failure mode this topic removes.
func (r *WebhookRetryRunner) Handle(ctx context.Context, rec kafka.Record) error {
	var msg webhookRetry
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		r.metric.Handled("skipped")
		r.logger.ErrorContext(ctx, "webhook retry: corrupt record skipped", "key", string(rec.Key), "err", err)
		return nil
	}
	accountID, err := uuid.Parse(msg.AccountID)
	if err != nil {
		r.metric.Handled("skipped")
		r.logger.ErrorContext(ctx, "webhook retry: unparseable account id, skipped",
			"event_id", msg.EventID, "err", err)
		return nil
	}

	// Wait only the time REMAINING until the event is due. Time already spent queued counts: an event that
	// waited behind others arrives due and is attempted at once. Sleeping the full backoff again here is
	// what would cap the drain near one event per backoff and let a dead account starve the accounts
	// behind it. Pacing before resolving also keeps a burst of deferrals from bursting control-plane reads.
	if err := r.pace(ctx, r.waitFor(msg)); err != nil {
		return err // context ended: leave the offset uncommitted, the record is redelivered
	}

	wh, found, err := r.webhooks.Get(ctx, accountID, msg.EventType)
	if err != nil {
		return err // a store outage is transient: redeliver rather than drop the event
	}
	if !found {
		// The webhook was deleted or disabled while the event waited. There is nothing left to deliver to.
		r.metric.Handled("dropped")
		r.logger.InfoContext(ctx, "webhook retry: webhook no longer resolvable, event dropped",
			"event_id", msg.EventID, "account_id", msg.AccountID, "event_type", msg.EventType)
		return nil
	}

	if err := r.sender.Retry(ctx, wh, webhook.Event{ID: msg.EventID, Payload: msg.Payload}, msg.Attempt, msg.FirstAttemptAt); err != nil {
		return err
	}
	r.metric.Handled("retried")
	if !msg.FirstAttemptAt.IsZero() {
		r.metric.Age(time.Since(msg.FirstAttemptAt))
	}
	return nil
}

// waitFor returns how long remains before msg is due. A record stamped with an absolute NotBefore waits
// only the remainder — zero if it already came due while queued. A record without one (written before the
// field existed) falls back to the full attempt backoff, so it is paced rather than hammered.
func (r *WebhookRetryRunner) waitFor(msg webhookRetry) time.Duration {
	if msg.NotBefore.IsZero() {
		return paceFor(msg.Attempt)
	}
	if d := time.Until(msg.NotBefore); d > 0 {
		return d
	}
	return 0
}

// paceFor returns how long to wait before re-attempting an event that has spent `attempt` attempts. It
// doubles per attempt from retryPaceBase and saturates at retryPaceMax.
func paceFor(attempt int) time.Duration {
	d := retryPaceBase
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= retryPaceMax {
			return retryPaceMax
		}
	}
	return d
}

// sleepCtx waits d, or returns early if ctx ends — so the runner drains promptly on shutdown instead of
// holding a partition through a long backoff.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
