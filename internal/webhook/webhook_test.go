package webhook_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

type parked struct {
	ev     webhook.Event
	reason string
}

type fakeSink struct {
	mu   sync.Mutex
	rows []parked
	err  error
}

func (s *fakeSink) Park(_ context.Context, _ cp.Webhook, ev webhook.Event, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.rows = append(s.rows, parked{ev, reason})
	return nil
}

func (s *fakeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

// testSender builds a sender whose backoff never waits and whose jitter is deterministic, so retry
// tests run instantly.
func testSender(sink webhook.DeadLetterSink, logger *slog.Logger) *webhook.Sender {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return webhook.NewSender(nil, sink, logger,
		webhook.WithSleep(func(context.Context, time.Duration) error { return nil }),
		webhook.WithJitter(func() float64 { return 0 }))
}

func webhookFor(url string) cp.Webhook {
	return cp.Webhook{
		ID: uuid.New(), AccountID: uuid.New(), EventType: cp.WebhookEventMO,
		URL: url, Secret: "s3cr3t", Status: cp.WebhookActive,
		RetryPolicyJSON: []byte(`{"max_attempts":3,"initial_backoff_ms":1}`),
	}
}

// TestSendDeliversWithVerifiableSignature: a 2xx delivery signs timestamp "." payload with HMAC-SHA256,
// and the receiver verifies it.
func TestSendDeliversWithVerifiableSignature(t *testing.T) {
	const secret = "s3cr3t"
	var gotSig, gotTS, gotID string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get(webhook.HeaderSignature)
		gotTS = r.Header.Get(webhook.HeaderTimestamp)
		gotID = r.Header.Get(webhook.HeaderEventID)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := &fakeSink{}
	ev := webhook.Event{ID: "evt-1", Payload: []byte(`{"mo":"hello"}`)}
	if err := testSender(sink, nil).Send(context.Background(), webhookFor(srv.URL), ev); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if !bytes.Equal(gotBody, ev.Payload) {
		t.Errorf("body = %q, want %q", gotBody, ev.Payload)
	}
	if gotID != "evt-1" {
		t.Errorf("event id header = %q, want evt-1", gotID)
	}
	want := "sha256=" + webhook.Sign(secret, gotTS, ev.Payload)
	if gotSig != want {
		t.Errorf("signature = %q, want %q", gotSig, want)
	}
	if sink.count() != 0 {
		t.Errorf("a delivered event must not be dead-lettered, got %d", sink.count())
	}
}

// TestSendRetriesThenSucceeds: two 500s then a 200 — delivered after retries, not dead-lettered.
func TestSendRetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := &fakeSink{}
	if err := testSender(sink, nil).Send(context.Background(), webhookFor(srv.URL),
		webhook.Event{ID: "e", Payload: []byte("x")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if attempts.Load() != 3 {
		t.Errorf("attempts = %d, want 3 (two retries)", attempts.Load())
	}
	if sink.count() != 0 {
		t.Errorf("eventual success must not dead-letter, got %d", sink.count())
	}
}

// TestSendDeadLettersAfterExhaustion: persistent 5xx exhausts the budget and the event is
// dead-lettered, once, with a status reason; Send reports success (handled).
func TestSendDeadLettersAfterExhaustion(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	sink := &fakeSink{}
	if err := testSender(sink, nil).Send(context.Background(), webhookFor(srv.URL),
		webhook.Event{ID: "e", Payload: []byte("x")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if attempts.Load() != 3 {
		t.Errorf("attempts = %d, want 3 (max_attempts)", attempts.Load())
	}
	if sink.count() != 1 {
		t.Fatalf("exhausted delivery must dead-letter once, got %d", sink.count())
	}
	if !strings.Contains(sink.rows[0].reason, "502") {
		t.Errorf("reason = %q, want it to mention the 502 status", sink.rows[0].reason)
	}
}

// TestSendDeadLettersOnPermanent4xx: a 4xx is permanent — dead-lettered on the first attempt, no
// retries.
func TestSendDeadLettersOnPermanent4xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	sink := &fakeSink{}
	if err := testSender(sink, nil).Send(context.Background(), webhookFor(srv.URL),
		webhook.Event{ID: "e", Payload: []byte("x")}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if attempts.Load() != 1 {
		t.Errorf("attempts = %d, want 1 (4xx is not retried)", attempts.Load())
	}
	if sink.count() != 1 {
		t.Errorf("a permanent rejection must dead-letter, got %d", sink.count())
	}
}

// TestSendNeverLogsBody: the payload never appears in the sender's log, even on the dead-letter path.
func TestSendNeverLogsBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	const body = "SECRET_MESSAGE_BODY"
	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	if err := testSender(&fakeSink{}, logger).Send(context.Background(), webhookFor(srv.URL),
		webhook.Event{ID: "e", Payload: []byte(body)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if strings.Contains(logBuf.String(), body) {
		t.Errorf("body leaked into the log (invariant a):\n%s", logBuf.String())
	}
}

// TestSendReturnsErrorWhenDeadLetterFails: if parking fails, Send returns an error so the caller
// reprocesses rather than losing the event.
func TestSendReturnsErrorWhenDeadLetterFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sink := &fakeSink{err: io.ErrUnexpectedEOF}
	if err := testSender(sink, nil).Send(context.Background(), webhookFor(srv.URL),
		webhook.Event{ID: "e", Payload: []byte("x")}); err == nil {
		t.Error("Send must return an error when the dead-letter sink fails")
	}
}

// TestSendContextCancelDuringBackoff: a cancelled context during backoff aborts without dead-lettering
// (the event stays on its source topic for reprocessing).
func TestSendContextCancelDuringBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sink := &fakeSink{}
	ctx, cancel := context.WithCancel(context.Background())
	// The backoff sleep cancels the context, simulating shutdown mid-retry.
	sender := webhook.NewSender(nil, sink, slog.New(slog.NewTextHandler(io.Discard, nil)),
		webhook.WithSleep(func(context.Context, time.Duration) error { cancel(); return ctx.Err() }),
		webhook.WithJitter(func() float64 { return 0 }))
	if err := sender.Send(ctx, webhookFor(srv.URL), webhook.Event{ID: "e", Payload: []byte("x")}); err == nil {
		t.Error("Send must return the context error when cancelled during backoff")
	}
	if sink.count() != 0 {
		t.Errorf("a cancelled send must not dead-letter, got %d", sink.count())
	}
}
