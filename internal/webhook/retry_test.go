package webhook_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// deferredCall records one Defer, so a test can assert what the sender handed to the retry topic.
type deferredCall struct {
	eventID  string
	attempt  int
	firstAt  time.Time
	reason   string
	deferred bool
}

type fakeRetrySink struct {
	calls []deferredCall
	err   error
}

func (f *fakeRetrySink) Defer(_ context.Context, _ cp.Webhook, ev webhook.Event, attempt int, firstAt time.Time, reason string) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, deferredCall{eventID: ev.ID, attempt: attempt, firstAt: firstAt, reason: reason, deferred: true})
	return nil
}

type parkCall struct {
	eventID string
	reason  string
}

type fakeParkSink struct{ calls []parkCall }

func (f *fakeParkSink) Park(_ context.Context, _ cp.Webhook, ev webhook.Event, reason string) error {
	f.calls = append(f.calls, parkCall{eventID: ev.ID, reason: reason})
	return nil
}

func testWebhook(t *testing.T, url string) cp.Webhook {
	t.Helper()
	return cp.Webhook{
		ID: uuid.New(), AccountID: uuid.New(), EventType: cp.WebhookEventMO,
		URL: url, Secret: "shhh", Status: cp.WebhookActive,
	}
}

// countingServer answers every request with status and counts the hits.
func countingServer(t *testing.T, status int) (*httptest.Server, *int) {
	t.Helper()
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// TestSendDefersInsteadOfBlocking is the point of step-192. With a retry sink wired, a transient failure
// makes Send hand the event to the retry topic after ONE attempt instead of sleeping through a backoff
// on the caller's goroutine. That goroutine is the delivery consumer, which processes records serially:
// sleeping there stalls the whole partition's return traffic behind one slow endpoint.
func TestSendDefersInsteadOfBlocking(t *testing.T) {
	srv, hits := countingServer(t, http.StatusInternalServerError)
	retry := &fakeRetrySink{}
	park := &fakeParkSink{}

	slept := false
	s := webhook.NewSender(srv.Client(), park, nil,
		webhook.WithRetrySink(retry),
		webhook.WithSleep(func(context.Context, time.Duration) error { slept = true; return nil }))

	if err := s.Send(context.Background(), testWebhook(t, srv.URL), webhook.Event{ID: "ev-1", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if *hits != 1 {
		t.Errorf("endpoint hit %d times, want exactly 1 — the hot path must not retry in band", *hits)
	}
	if slept {
		t.Error("Send slept on the caller's goroutine — that is the head-of-line blocking this change removes")
	}
	if len(retry.calls) != 1 {
		t.Fatalf("deferred %d event(s), want 1", len(retry.calls))
	}
	if retry.calls[0].attempt != 1 {
		t.Errorf("deferred with attempt = %d, want 1 (the first attempt was just spent)", retry.calls[0].attempt)
	}
	if len(park.calls) != 0 {
		t.Errorf("dead-lettered %+v — a transient failure must be retried, not abandoned", park.calls)
	}
}

// TestSendDeadLettersPermanentWithoutDeferring proves the transient/permanent split survives. A 4xx means
// the endpoint refuses this payload; queueing it for retry would burn the retry topic on work that cannot
// succeed, so it goes straight to the dead-letter.
func TestSendDeadLettersPermanentWithoutDeferring(t *testing.T) {
	srv, hits := countingServer(t, http.StatusBadRequest)
	retry := &fakeRetrySink{}
	park := &fakeParkSink{}

	s := webhook.NewSender(srv.Client(), park, nil, webhook.WithRetrySink(retry))
	if err := s.Send(context.Background(), testWebhook(t, srv.URL), webhook.Event{ID: "ev-2", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if *hits != 1 {
		t.Errorf("endpoint hit %d times, want 1", *hits)
	}
	if len(retry.calls) != 0 {
		t.Errorf("deferred a permanently-rejected event %+v — it can never succeed", retry.calls)
	}
	if len(park.calls) != 1 {
		t.Fatalf("dead-lettered %d event(s), want 1", len(park.calls))
	}
}

// TestSendWithoutRetrySinkKeepsInlineBehaviour is the safe default: a caller that wires no retry sink
// keeps the pre-existing in-band retry loop, so this change cannot alter an unmigrated call site.
func TestSendWithoutRetrySinkKeepsInlineBehaviour(t *testing.T) {
	srv, hits := countingServer(t, http.StatusInternalServerError)
	park := &fakeParkSink{}

	s := webhook.NewSender(srv.Client(), park, nil,
		webhook.WithMaxAttempts(3),
		webhook.WithSleep(func(context.Context, time.Duration) error { return nil }))

	if err := s.Send(context.Background(), testWebhook(t, srv.URL), webhook.Event{ID: "ev-3", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if *hits != 3 {
		t.Errorf("endpoint hit %d times, want 3 (the inline loop is unchanged without a retry sink)", *hits)
	}
	if len(park.calls) != 1 {
		t.Errorf("dead-lettered %d, want 1 after exhaustion", len(park.calls))
	}
}

// TestRetrySucceedsOnLaterAttempt proves the delivery a deferred retry buys back: an endpoint that was
// briefly down is delivered to on a later pass, where the old inline design would have dead-lettered it.
func TestRetrySucceedsOnLaterAttempt(t *testing.T) {
	srv, hits := countingServer(t, http.StatusOK)
	retry := &fakeRetrySink{}
	park := &fakeParkSink{}

	s := webhook.NewSender(srv.Client(), park, nil, webhook.WithRetrySink(retry))
	err := s.Retry(context.Background(), testWebhook(t, srv.URL), webhook.Event{ID: "ev-4", Payload: []byte(`{}`)},
		2, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}

	if *hits != 1 {
		t.Errorf("endpoint hit %d times, want 1", *hits)
	}
	if len(retry.calls) != 0 || len(park.calls) != 0 {
		t.Errorf("a delivered event was re-queued (%+v) or parked (%+v)", retry.calls, park.calls)
	}
}

// TestRetryRedefersWithIncrementedAttempt proves the attempt counter advances across passes and the
// original first-attempt instant is carried through — that instant is what bounds the total lifetime,
// independently of how many passes occurred.
func TestRetryRedefersWithIncrementedAttempt(t *testing.T) {
	srv, _ := countingServer(t, http.StatusServiceUnavailable)
	retry := &fakeRetrySink{}
	firstAt := time.Now().Add(-time.Minute)

	s := webhook.NewSender(srv.Client(), &fakeParkSink{}, nil, webhook.WithRetrySink(retry))
	if err := s.Retry(context.Background(), testWebhook(t, srv.URL), webhook.Event{ID: "ev-5", Payload: []byte(`{}`)}, 2, firstAt); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	if len(retry.calls) != 1 {
		t.Fatalf("re-deferred %d, want 1", len(retry.calls))
	}
	if retry.calls[0].attempt != 3 {
		t.Errorf("attempt = %d, want 3 (2 spent plus this one)", retry.calls[0].attempt)
	}
	if !retry.calls[0].firstAt.Equal(firstAt) {
		t.Errorf("firstAt = %v, want the original %v — the age bound must not reset each pass", retry.calls[0].firstAt, firstAt)
	}
}

// TestRetryParksWhenTooOld is the termination guard. A permanently-unreachable endpoint must not cycle on
// the retry topic forever: past the maximum age the event is dead-lettered, whatever its attempt count.
func TestRetryParksWhenTooOld(t *testing.T) {
	srv, _ := countingServer(t, http.StatusServiceUnavailable)
	retry := &fakeRetrySink{}
	park := &fakeParkSink{}

	s := webhook.NewSender(srv.Client(), park, nil,
		webhook.WithRetrySink(retry), webhook.WithMaxRetryAge(30*time.Minute))
	err := s.Retry(context.Background(), testWebhook(t, srv.URL), webhook.Event{ID: "ev-6", Payload: []byte(`{}`)},
		2, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("Retry: %v", err)
	}

	if len(retry.calls) != 0 {
		t.Errorf("re-deferred an over-age event %+v — it would cycle forever", retry.calls)
	}
	if len(park.calls) != 1 {
		t.Fatalf("dead-lettered %d, want 1", len(park.calls))
	}
}

// TestRetryParksWhenAttemptsExhausted proves the second termination bound: the webhook's own attempt
// budget still applies across passes, so a fast-failing endpoint stops before the age limit.
func TestRetryParksWhenAttemptsExhausted(t *testing.T) {
	srv, _ := countingServer(t, http.StatusServiceUnavailable)
	retry := &fakeRetrySink{}
	park := &fakeParkSink{}

	s := webhook.NewSender(srv.Client(), park, nil,
		webhook.WithRetrySink(retry), webhook.WithMaxAttempts(3))
	if err := s.Retry(context.Background(), testWebhook(t, srv.URL), webhook.Event{ID: "ev-7", Payload: []byte(`{}`)}, 3, time.Now()); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	if len(retry.calls) != 0 {
		t.Errorf("re-deferred past the attempt budget %+v", retry.calls)
	}
	if len(park.calls) != 1 {
		t.Fatalf("dead-lettered %d, want 1", len(park.calls))
	}
}

// TestDeferFailureSurfaces proves a retry-topic outage is reported rather than swallowed. The caller must
// NOT commit the source offset in that case: silently dropping the event would lose a return-path
// delivery the customer is entitled to.
func TestDeferFailureSurfaces(t *testing.T) {
	srv, _ := countingServer(t, http.StatusInternalServerError)
	retry := &fakeRetrySink{err: errors.New("kafka down")}

	s := webhook.NewSender(srv.Client(), &fakeParkSink{}, nil, webhook.WithRetrySink(retry))
	if err := s.Send(context.Background(), testWebhook(t, srv.URL), webhook.Event{ID: "ev-8", Payload: []byte(`{}`)}); err == nil {
		t.Fatal("Send = nil although the retry topic rejected the event — the caller would commit and lose it")
	}
}

// TestDeferredRetryIsSignedFreshly proves the signature stays verifiable across a deferred retry. The
// signature covers timestamp "." payload and is computed at send time, so a retry minutes later carries a
// current timestamp — a receiver enforcing a timestamp freshness window still accepts it.
func TestDeferredRetryIsSignedFreshly(t *testing.T) {
	var gotTS, gotSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTS, gotSig = r.Header.Get(webhook.HeaderTimestamp), r.Header.Get(webhook.HeaderSignature)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	now := time.Now()
	wh := testWebhook(t, srv.URL)
	payload := []byte(`{"id":"ev-9"}`)

	s := webhook.NewSender(srv.Client(), &fakeParkSink{}, nil,
		webhook.WithRetrySink(&fakeRetrySink{}), webhook.WithClock(func() time.Time { return now }))
	// An event first attempted an hour ago, retried now.
	if err := s.Retry(context.Background(), wh, webhook.Event{ID: "ev-9", Payload: payload}, 2, now.Add(-time.Hour)); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	if want := strconv.FormatInt(now.Unix(), 10); gotTS != want {
		t.Errorf("timestamp = %q, want the send-time %q, not the original attempt's", gotTS, want)
	}
	if want := "sha256=" + webhook.Sign(wh.Secret, gotTS, payload); gotSig != want {
		t.Errorf("signature = %q, want %q — a receiver would reject the retry", gotSig, want)
	}
}
