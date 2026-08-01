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

// WebhookRetryRunner drains webhook.retry: it re-resolves each queued event's webhook, waits out a paced
// backoff, and hands it back to the sender for one more attempt (step-192).
//
// It runs on its OWN consumer group and goroutine, which is the whole point: the delivery consumer must
// never wait on a slow endpoint. Consequence to know when operating it: because the runner paces before
// each attempt, its throughput is bounded — this topic is a recovery path, not a delivery path, and a
// large backlog drains slowly by design.
type WebhookRetryRunner struct {
	webhooks WebhookGetter
	sender   RetrySender
	pace     func(ctx context.Context, d time.Duration) error
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
	r := &WebhookRetryRunner{webhooks: webhooks, sender: sender, pace: sleepCtx, logger: slog.Default()}
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
		r.logger.ErrorContext(ctx, "webhook retry: corrupt record skipped", "key", string(rec.Key), "err", err)
		return nil
	}
	accountID, err := uuid.Parse(msg.AccountID)
	if err != nil {
		r.logger.ErrorContext(ctx, "webhook retry: unparseable account id, skipped",
			"event_id", msg.EventID, "err", err)
		return nil
	}

	// Wait BEFORE resolving and attempting: pacing is the runner's reason to exist, and doing it first
	// also means a burst of deferrals cannot turn into a burst of control-plane reads.
	if err := r.pace(ctx, paceFor(msg.Attempt)); err != nil {
		return err // context ended: leave the offset uncommitted, the record is redelivered
	}

	wh, found, err := r.webhooks.Get(ctx, accountID, msg.EventType)
	if err != nil {
		return err // a store outage is transient: redeliver rather than drop the event
	}
	if !found {
		// The webhook was deleted or disabled while the event waited. There is nothing left to deliver to.
		r.logger.InfoContext(ctx, "webhook retry: webhook no longer resolvable, event dropped",
			"event_id", msg.EventID, "account_id", msg.AccountID, "event_type", msg.EventType)
		return nil
	}

	return r.sender.Retry(ctx, wh, webhook.Event{ID: msg.EventID, Payload: msg.Payload}, msg.Attempt, msg.FirstAttemptAt)
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
