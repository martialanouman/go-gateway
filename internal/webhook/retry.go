package webhook

import (
	"context"
	"fmt"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// defaultMaxRetryAge bounds how long a deferred event may keep cycling on the retry topic before it is
// dead-lettered, counted from its FIRST attempt. It is the termination guard for an endpoint that is down
// for good: the attempt budget alone is not enough, since a slow-failing endpoint could otherwise stretch
// a handful of attempts over days.
const defaultMaxRetryAge = 6 * time.Hour

// RetrySink hands a transiently-failed event to a durable retry queue, to be attempted again later off
// the caller's goroutine. attempt is how many attempts have been SPENT (so the next pass is attempt+1) and
// firstAt is when the event was first tried — the age bound is measured from it, so it must be carried
// unchanged across passes rather than reset each time.
//
// Implementations must NOT persist wh.Secret: a queued record is operator-visible, and the signing key
// must not leak (the dead-letter sink follows the same rule). The consumer re-resolves the webhook, and
// its secret, from the control plane.
type RetrySink interface {
	Defer(ctx context.Context, wh cp.Webhook, ev Event, attempt int, firstAt time.Time, reason string) error
}

// WithRetrySink switches the sender from IN-BAND retries to deferred ones: Send makes a single attempt and
// hands a transient failure to sink instead of sleeping through a backoff on the caller's goroutine.
//
// That goroutine is a Kafka consumer processing records serially, so an in-band backoff stalls a whole
// partition's return traffic behind one slow endpoint (head-of-line blocking). Deferring also RECOVERS
// deliveries the bounded in-band loop used to abandon: a briefly-unreachable endpoint is retried on a
// later pass rather than dead-lettered within seconds.
//
// Without this option the sender keeps its original in-band loop, so an un-migrated call site is unaffected.
func WithRetrySink(sink RetrySink) Option {
	return func(s *Sender) {
		if sink != nil {
			s.retry = sink
		}
	}
}

// WithMaxRetryAge overrides how long a deferred event may keep being retried, measured from its first
// attempt (default 6h). It is what stops an event cycling forever against a permanently-dead endpoint.
func WithMaxRetryAge(d time.Duration) Option {
	return func(s *Sender) {
		if d > 0 {
			s.maxRetryAge = d
		}
	}
}

// Retry makes one further attempt at a previously-deferred event. It is what the retry-topic consumer
// calls. attempt is the number of attempts already spent and firstAt the original attempt instant.
//
// The event reaches a terminal state — delivered, or dead-lettered on a permanent rejection, an exhausted
// attempt budget, or an exceeded age — and Retry returns nil. It returns an error only when the work could
// not be completed and must be retried by redelivering the record: the retry sink or the dead-letter sink
// itself failed. The caller must not commit its offset in that case, or the event is lost.
func (s *Sender) Retry(ctx context.Context, wh cp.Webhook, ev Event, attempt int, firstAt time.Time) error {
	return s.deliverOnce(ctx, wh, ev, attempt, firstAt)
}

// deliverOnce performs a single attempt and routes the outcome: delivered ends it, a permanent rejection
// dead-letters, and a transient failure is deferred unless a termination bound has been reached.
func (s *Sender) deliverOnce(ctx context.Context, wh cp.Webhook, ev Event, spent int, firstAt time.Time) error {
	outcome, reason := s.attempt(ctx, wh, ev)
	switch outcome {
	case outcomeDelivered:
		return nil
	case outcomePermanent:
		return s.park(ctx, wh, ev, reason)
	}

	spent++
	if done, why := s.retriesExhausted(wh, spent, firstAt); done {
		// Both bounds end in the dead-letter, never in a silent drop: the event stays recoverable.
		return s.park(ctx, wh, ev, reason+" ("+why+")")
	}
	if err := s.retry.Defer(ctx, wh, ev, spent, firstAt, reason); err != nil {
		return fmt.Errorf("webhook: defer %s: %w", ev.ID, err)
	}
	return nil
}

// retriesExhausted reports whether a deferred event has hit a termination bound, and which one. The
// attempt budget stops a fast-failing endpoint; the age bound stops a slow-failing one, which could
// otherwise stretch few attempts over days.
func (s *Sender) retriesExhausted(wh cp.Webhook, spent int, firstAt time.Time) (bool, string) {
	policy := parseRetryPolicy(wh.RetryPolicyJSON)
	maxAttempts := policy.MaxAttempts
	if s.maxAttempts > 0 && s.maxAttempts < maxAttempts {
		maxAttempts = s.maxAttempts
	}
	if spent >= maxAttempts {
		return true, "attempt budget exhausted"
	}
	// A zero firstAt (an event deferred before this field existed) must not read as "infinitely old".
	if !firstAt.IsZero() && s.now().Sub(firstAt) >= s.maxRetryAge {
		return true, "max retry age exceeded"
	}
	return false, ""
}
