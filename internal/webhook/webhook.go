// Package webhook is the return-path webhook sender: it POSTs an MO or DLR event to an account's
// configured URL, signed with HMAC-SHA256, retrying with exponential backoff and jitter and
// dead-lettering the event once the retry budget is spent. It uses only the standard library
// (crypto/hmac, net/http). The event payload may legitimately carry the message body (the recipient
// is the account's own endpoint), but the sender NEVER logs it (invariant a): only ids, the webhook
// URL and the failure reason are observable.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// The signature headers. X-Webhook-Signature is "sha256=" + hex(HMAC-SHA256(secret, timestamp "."
// payload)); the timestamp is also sent as X-Webhook-Timestamp so the receiver can reject a replay,
// and X-Webhook-Id is the stable event id the receiver dedups on.
const (
	HeaderSignature = "X-Webhook-Signature"
	HeaderTimestamp = "X-Webhook-Timestamp"
	HeaderEventID   = "X-Webhook-Id"
)

// Event is one return-path event to deliver. Payload is the exact bytes signed and POSTed (it may
// contain the message body — never log it). ID is a stable identifier the receiver dedups on.
type Event struct {
	ID      string
	Payload []byte
}

// DeadLetterSink parks an event whose delivery is abandoned (retries exhausted, or a permanent
// rejection), so it is never lost. The concrete sink (a Kafka dead-letter topic or a table) is wired
// by the caller (step-048).
type DeadLetterSink interface {
	Park(ctx context.Context, wh cp.Webhook, ev Event, reason string) error
}

// Sender delivers events to webhooks. It is safe for concurrent use.
type Sender struct {
	client     *http.Client
	deadLetter DeadLetterSink
	logger     *slog.Logger
	now        func() time.Time
	sleep      func(ctx context.Context, d time.Duration) error
	jitter     func() float64
}

// Option overrides a Sender default (the clock, the backoff sleep, the jitter source) for tests.
type Option func(*Sender)

// WithClock overrides the timestamp source.
func WithClock(now func() time.Time) Option { return func(s *Sender) { s.now = now } }

// WithSleep overrides the backoff wait (a test passes a no-op to avoid real delays).
func WithSleep(sleep func(ctx context.Context, d time.Duration) error) Option {
	return func(s *Sender) { s.sleep = sleep }
}

// WithJitter overrides the [0,1) jitter source for deterministic tests.
func WithJitter(j func() float64) Option { return func(s *Sender) { s.jitter = j } }

// NewSender builds a sender. A nil client defaults to one with a strict per-request timeout; a nil
// logger to slog.Default; a nil dead-letter sink to a no-op (the event is dropped after exhaustion —
// wire a real sink in production).
func NewSender(client *http.Client, deadLetter DeadLetterSink, logger *slog.Logger, opts ...Option) *Sender {
	if client == nil {
		client = &http.Client{
			Timeout: 10 * time.Second,
			// Never follow a redirect: the destination URL is account-controlled, so following a 3xx would
			// let an endpoint bounce the signed POST to an internal address (cloud metadata, localhost),
			// bypassing any URL validation. A 3xx is returned as the final response (classified permanent).
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	if deadLetter == nil {
		deadLetter = noopSink{}
	}
	s := &Sender{
		client:     client,
		deadLetter: deadLetter,
		logger:     logger,
		now:        time.Now,
		sleep:      sleepCtx,
		jitter:     rand.Float64,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Send delivers ev to wh, retrying per the webhook's retry policy. It returns nil once the event
// reaches a terminal state — delivered (2xx), or dead-lettered after a permanent rejection or an
// exhausted retry budget. It returns an error only when the work could not be completed and should be
// retried later: the context ended, or the dead-letter sink itself failed. The payload is never
// logged. The caller is responsible for having resolved an active webhook — Send does not consult
// wh.Status (whether a disabled webhook suppresses delivery is the resolver's decision, step-048).
func (s *Sender) Send(ctx context.Context, wh cp.Webhook, ev Event) error {
	policy := parseRetryPolicy(wh.RetryPolicyJSON)
	backoff := policy.InitialBackoff

	for attempt := 1; ; attempt++ {
		outcome, reason := s.attempt(ctx, wh, ev)
		switch outcome {
		case outcomeDelivered:
			return nil
		case outcomePermanent:
			return s.park(ctx, wh, ev, reason)
		default: // outcomeRetryable
			if attempt >= policy.MaxAttempts {
				return s.park(ctx, wh, ev, reason)
			}
			if err := s.sleep(ctx, applyJitter(backoff, s.jitter())); err != nil {
				// The context ended during backoff: not delivered, not dead-lettered — the caller
				// reprocesses (the event is still on its source topic).
				return err
			}
			backoff = nextBackoff(backoff, policy)
		}
	}
}

type outcome uint8

const (
	outcomeDelivered outcome = iota
	outcomeRetryable
	outcomePermanent
)

// attempt performs one signed POST and classifies the result. A 2xx is delivered; a 429 or 5xx is
// retryable (transient); any other 4xx is permanent (retrying will not help); a transport error is
// retryable. The reason string never contains the payload.
func (s *Sender) attempt(ctx context.Context, wh cp.Webhook, ev Event) (outcome, string) {
	req, err := s.buildRequest(ctx, wh, ev)
	if err != nil {
		// A malformed URL cannot be fixed by retrying.
		return outcomePermanent, "build request: " + err.Error()
	}
	resp, err := s.client.Do(req)
	if err != nil {
		// err is a *url.Error: method + URL + network cause, never the payload. The URL is observable
		// (see the package doc), so it is safe — and useful — in the dead-letter reason.
		return outcomeRetryable, "transport error: " + err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain a bounded amount so the connection can be reused, without letting a hostile endpoint stream
	// unbounded data at us (capped anyway by the client timeout).
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return outcomeDelivered, ""
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return outcomeRetryable, fmt.Sprintf("http status %d", resp.StatusCode)
	default:
		return outcomePermanent, fmt.Sprintf("http status %d", resp.StatusCode)
	}
}

// buildRequest builds the signed POST. The signature covers timestamp "." payload, so a captured
// request cannot be replayed with a new timestamp.
func (s *Sender) buildRequest(ctx context.Context, wh cp.Webhook, ev Event) (*http.Request, error) {
	ts := strconv.FormatInt(s.now().Unix(), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, wh.URL, bytes.NewReader(ev.Payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderEventID, ev.ID)
	req.Header.Set(HeaderTimestamp, ts)
	req.Header.Set(HeaderSignature, "sha256="+Sign(wh.Secret, ts, ev.Payload))
	return req, nil
}

// park dead-letters an abandoned event, logging the outcome (ids + reason, never the payload).
func (s *Sender) park(ctx context.Context, wh cp.Webhook, ev Event, reason string) error {
	if err := s.deadLetter.Park(ctx, wh, ev, reason); err != nil {
		return fmt.Errorf("webhook: dead-letter %s: %w", ev.ID, err)
	}
	s.logger.WarnContext(ctx, "webhook: event dead-lettered",
		"event_id", ev.ID, "account_id", wh.AccountID, "event_type", wh.EventType, "reason", reason)
	return nil
}

// Sign computes the hex HMAC-SHA256 of timestamp "." payload under secret. It is exported so a
// receiver (and the test) verify the same construction.
func Sign(secret, timestamp string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// applyJitter returns a wait between 50% and 100% of d (equal jitter), so concurrent senders do not
// retry in lockstep. r is in [0,1).
func applyJitter(d time.Duration, r float64) time.Duration {
	return d/2 + time.Duration(r*float64(d/2))
}

// nextBackoff grows the backoff by the policy multiplier, capped at MaxBackoff.
func nextBackoff(current time.Duration, p RetryPolicy) time.Duration {
	next := time.Duration(float64(current) * p.Multiplier)
	if next > p.MaxBackoff {
		return p.MaxBackoff
	}
	return next
}

// sleepCtx waits for d or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// noopSink drops an event after exhaustion. The default only; production wires a durable sink.
type noopSink struct{}

func (noopSink) Park(context.Context, cp.Webhook, Event, string) error { return nil }

// RetryPolicy bounds the delivery retries. It is parsed from the webhook's retry_policy_json.
type RetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// defaultRetryPolicy is used when retry_policy_json is empty or malformed: five attempts, 1s growing
// by 2× to a 30s cap.
func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 5, InitialBackoff: time.Second, MaxBackoff: 30 * time.Second, Multiplier: 2.0}
}

// parseRetryPolicy reads retry_policy_json, falling back to defaults for missing, malformed or
// out-of-range fields (a bad policy must not stop delivery). JSON shape:
// {"max_attempts":5,"initial_backoff_ms":1000,"max_backoff_ms":30000,"multiplier":2.0}.
func parseRetryPolicy(raw json.RawMessage) RetryPolicy {
	p := defaultRetryPolicy()
	if len(raw) == 0 {
		return p
	}
	var j struct {
		MaxAttempts *int     `json:"max_attempts"`
		InitialMs   *int     `json:"initial_backoff_ms"`
		MaxMs       *int     `json:"max_backoff_ms"`
		Multiplier  *float64 `json:"multiplier"`
	}
	if err := json.Unmarshal(raw, &j); err != nil {
		return p
	}
	if j.MaxAttempts != nil && *j.MaxAttempts > 0 {
		p.MaxAttempts = min(*j.MaxAttempts, maxAttemptsCap)
	}
	if j.InitialMs != nil && *j.InitialMs > 0 {
		p.InitialBackoff = time.Duration(min(*j.InitialMs, maxBackoffMs)) * time.Millisecond
	}
	if j.MaxMs != nil && *j.MaxMs > 0 {
		p.MaxBackoff = time.Duration(min(*j.MaxMs, maxBackoffMs)) * time.Millisecond
	}
	if j.Multiplier != nil && *j.Multiplier >= 1 {
		p.Multiplier = *j.Multiplier
	}
	if p.MaxBackoff < p.InitialBackoff {
		p.MaxBackoff = p.InitialBackoff
	}
	return p
}

// The retry-policy caps: a misconfigured retry_policy_json must not be able to block a delivery
// goroutine for an unbounded time (or overflow a duration). 20 attempts and a 5-minute ceiling are far
// above any sane policy.
const (
	maxAttemptsCap = 20
	maxBackoffMs   = 5 * 60 * 1000
)
