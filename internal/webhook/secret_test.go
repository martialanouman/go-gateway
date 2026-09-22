package webhook_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

type fakeOpener struct {
	plaintext string
	err       error
	calls     atomic.Int32
	got       cp.SealedSecret
}

func (o *fakeOpener) Open(_ context.Context, sealed cp.SealedSecret) ([]byte, error) {
	o.calls.Add(1)
	o.got = sealed
	if o.err != nil {
		return nil, o.err
	}
	return []byte(o.plaintext), nil
}

func TestSendSignsWithTheOpenedSecret(t *testing.T) {
	const plaintext = "the-real-signing-key"
	var gotSig, gotTS string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get(webhook.HeaderSignature)
		gotTS = r.Header.Get(webhook.HeaderTimestamp)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	opener := &fakeOpener{plaintext: plaintext}
	wh := webhookFor(srv.URL)
	ev := webhook.Event{ID: "evt-sealed", Payload: []byte(`{"mo":"hello"}`)}
	if err := senderWith(&fakeSink{}, opener, nil).Send(context.Background(), wh, ev); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if !bytes.Equal(opener.got.Sealed, wh.Secret.Sealed) || opener.got.KMSKeyRef != wh.Secret.KMSKeyRef {
		t.Errorf("opened %+v, want the webhook's stored pair %+v", opener.got, wh.Secret)
	}
	if want := "sha256=" + independentSignature(plaintext, gotTS, gotBody); gotSig != want {
		t.Errorf("signature = %q, want %q", gotSig, want)
	}
	if got := opener.calls.Load(); got != 1 {
		t.Errorf("opened %d times, want exactly 1 per delivery", got)
	}
}

// independentSignature recomputes the scheme by hand rather than through webhook.Sign, so the assertions
// above pin the WIRE FORMAT a third-party receiver implements — not merely that Sign agrees with itself.
// Dropping the timestamp or the separator from Sign leaves every Sign-based expectation green.
func independentSignature(secret, timestamp string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// The in-band loop is the reason openSecret sits before the retry-sink branch: one open must serve every
// attempt of a delivery, or a failing endpoint costs one gRPC round trip per attempt.
func TestSendOpensOncePerDeliveryNotPerAttempt(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	opener := &fakeOpener{plaintext: "s3cr3t"}
	if err := senderWith(&fakeSink{}, opener, nil).Send(context.Background(), webhookFor(srv.URL),
		webhook.Event{ID: "evt-multi", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("endpoint hit %d times, want 3 — the test needs a multi-attempt delivery to mean anything", got)
	}
	if got := opener.calls.Load(); got != 1 {
		t.Errorf("opened %d times across 3 attempts, want 1", got)
	}
}

func TestSendRedeliversWhenTheKeyServiceIsUnreachable(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink, retry := &fakeSink{}, &fakeRetrySink{}
	opener := &fakeOpener{err: fmt.Errorf("dial content key: %w", errs.ErrServiceUnavailable)}
	err := senderWith(sink, opener, nil, webhook.WithRetrySink(retry)).Send(context.Background(),
		webhookFor(srv.URL), webhook.Event{ID: "evt-down", Payload: []byte(`{}`)})

	if err == nil {
		t.Fatal("Send returned nil: the record would be committed and the event lost")
	}
	if !errors.Is(err, errs.ErrServiceUnavailable) {
		t.Errorf("err = %v, want it to carry ErrServiceUnavailable", err)
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("endpoint hit %d times, want 0 — nothing can be signed without the secret", got)
	}
	if sink.count() != 0 || len(retry.calls) != 0 {
		t.Errorf("dead-lettered %d and deferred %d, want neither: no attempt was spent",
			sink.count(), len(retry.calls))
	}
}

// A ciphertext that does not open fails identically on every redelivery. Returning an error would hold the
// partition, and with it every other account's MO and DLR, on one unopenable row.
func TestSendParksAnUnopenableSecretInsteadOfBlockingTheConsumer(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink, retry := &fakeSink{}, &fakeRetrySink{}
	opener := &fakeOpener{err: fmt.Errorf("unwrap: %w", errs.ErrInternal)}
	if err := senderWith(sink, opener, nil, webhook.WithRetrySink(retry)).Send(context.Background(),
		webhookFor(srv.URL), webhook.Event{ID: "evt-broken", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Send = %v, want nil: a deterministic fault must not be redelivered forever", err)
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("endpoint hit %d times, want 0", got)
	}
	if len(retry.calls) != 0 {
		t.Errorf("deferred %d times, want 0: deferring only delays the same failure", len(retry.calls))
	}
	if sink.count() != 1 {
		t.Fatalf("dead-lettered %d events, want exactly 1", sink.count())
	}
	reason := sink.reasons()[0]
	if !strings.Contains(reason, "secret_unopenable") {
		t.Errorf("park reason = %q, want it to name secret_unopenable", reason)
	}
	// The reason travels into a durable, operator-visible Kafka record, and it is built from the opener's
	// error chain.
	wh := webhookFor(srv.URL)
	for probe, needle := range map[string]string{
		"sealed in base64": base64.StdEncoding.EncodeToString(wh.Secret.Sealed),
		"key reference":    wh.Secret.KMSKeyRef,
	} {
		if strings.Contains(reason, needle) {
			t.Errorf("the park reason carries the %s: %q", probe, reason)
		}
	}
}

func TestRetryClassifiesAFailedOpenLikeSendDoes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	wh := webhookFor(srv.URL)
	ev := webhook.Event{ID: "evt-deferred", Payload: []byte(`{}`)}

	t.Run("unreachable key service is redelivered", func(t *testing.T) {
		park, retry := &fakeParkSink{}, &fakeRetrySink{}
		s := senderWith(park, &fakeOpener{err: fmt.Errorf("dial: %w", errs.ErrServiceUnavailable)}, nil,
			webhook.WithRetrySink(retry))
		if err := s.Retry(context.Background(), wh, ev, 1, time.Now()); err == nil {
			t.Fatal("Retry returned nil: the consumer would commit and the event would be lost")
		}
		if len(park.calls) != 0 || len(retry.calls) != 0 {
			t.Errorf("parked %d / deferred %d, want neither: the attempt never happened",
				len(park.calls), len(retry.calls))
		}
	})

	t.Run("unopenable ciphertext is parked", func(t *testing.T) {
		park, retry := &fakeParkSink{}, &fakeRetrySink{}
		s := senderWith(park, &fakeOpener{err: fmt.Errorf("unwrap: %w", errs.ErrInternal)}, nil,
			webhook.WithRetrySink(retry))
		if err := s.Retry(context.Background(), wh, ev, 1, time.Now()); err != nil {
			t.Fatalf("Retry = %v, want nil: this record can never succeed", err)
		}
		if len(retry.calls) != 0 {
			t.Errorf("deferred %d times, want 0", len(retry.calls))
		}
		if len(park.calls) != 1 || !strings.Contains(park.calls[0].reason, "secret_unopenable") {
			t.Errorf("park calls = %+v, want one naming secret_unopenable", park.calls)
		}
	})
}

// Park is the retry runner's only way to dead-letter a switched-off webhook's event without destroying the
// backlog. It must not need the secret: opening there would make an unreachable key service stall the very
// drain that exists to empty the backlog.
func TestParkNeverOpensTheSecret(t *testing.T) {
	park := &fakeParkSink{}
	opener := &fakeOpener{err: errors.New("Park must not have called this")}
	s := senderWith(park, opener, nil)

	if err := s.Park(context.Background(), webhookFor("https://unused.test"),
		webhook.Event{ID: "evt-off", Payload: []byte(`{}`)}, "webhook_disabled"); err != nil {
		t.Fatalf("Park: %v", err)
	}
	if got := opener.calls.Load(); got != 0 {
		t.Errorf("Park opened the secret %d times, want 0 — it signs nothing", got)
	}
	if len(park.calls) != 1 {
		t.Errorf("parked %d events, want 1", len(park.calls))
	}
}

// An opener that answers no error and no plaintext would have every delivery signed with HMAC-SHA256("") —
// a signature any receiver rejects, reported as an HTTP failure that names nothing about the real cause.
func TestSendRefusesASecretThatOpensEmpty(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := &fakeSink{}
	if err := senderWith(sink, &fakeOpener{plaintext: ""}, nil).Send(context.Background(),
		webhookFor(srv.URL), webhook.Event{ID: "evt-empty", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Send = %v, want nil (dead-lettered)", err)
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("endpoint hit %d times, want 0 — an empty key must never reach the wire", got)
	}
	if sink.count() != 1 {
		t.Errorf("dead-lettered %d events, want 1", sink.count())
	}
}
