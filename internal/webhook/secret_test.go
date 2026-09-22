package webhook_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
}

func (o *fakeOpener) Open(_ context.Context, sealed cp.SealedSecret) ([]byte, error) {
	o.calls.Add(1)
	if o.err != nil {
		return nil, o.err
	}
	return []byte(o.plaintext), nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func senderWithOpener(sink webhook.DeadLetterSink, opener webhook.SecretOpener, opts ...webhook.Option) *webhook.Sender {
	return webhook.NewSender(nil, sink, opener, discardLogger(),
		append([]webhook.Option{
			webhook.WithSleep(func(context.Context, time.Duration) error { return nil }),
			webhook.WithJitter(func() float64 { return 0 }),
		}, opts...)...)
}

func TestSendSignsWithTheOpenedSecretAndNotTheStoredBytes(t *testing.T) {
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
	if err := senderWithOpener(&fakeSink{}, opener).Send(context.Background(), wh, ev); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if want := "sha256=" + webhook.Sign(plaintext, gotTS, gotBody); gotSig != want {
		t.Errorf("signature = %q, want %q — signed with something other than the opened secret", gotSig, want)
	}
	if gotSig == "sha256="+webhook.Sign(string(wh.Secret.Sealed), gotTS, gotBody) {
		t.Error("the sender signed with the SEALED bytes: the receiver could never verify that")
	}
	if got := opener.calls.Load(); got != 1 {
		t.Errorf("opened %d times, want exactly 1 per delivery", got)
	}
}

func TestSendReturnsTheErrorWhenTheKeyServiceIsUnreachable(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := &fakeSink{}
	opener := &fakeOpener{err: fmt.Errorf("dial content key: %w", errs.ErrServiceUnavailable)}
	err := senderWithOpener(sink, opener).Send(context.Background(), webhookFor(srv.URL),
		webhook.Event{ID: "evt-down", Payload: []byte(`{}`)})

	if err == nil {
		t.Fatal("Send returned nil: the record would be committed and the event lost")
	}
	if !errors.Is(err, errs.ErrServiceUnavailable) {
		t.Errorf("err = %v, want it to carry ErrServiceUnavailable", err)
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("endpoint hit %d times, want 0 — nothing can be signed without the secret", got)
	}
	if sink.count() != 0 {
		t.Errorf("dead-lettered %d events, want 0: the fault is ours and transient", sink.count())
	}
}

func TestSendParksAnUnopenableSecretInsteadOfBlockingTheConsumer(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := &fakeSink{}
	opener := &fakeOpener{err: fmt.Errorf("unwrap: %w", errs.ErrInternal)}
	if err := senderWithOpener(sink, opener).Send(context.Background(), webhookFor(srv.URL),
		webhook.Event{ID: "evt-broken", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("Send = %v, want nil: a deterministic fault must not be redelivered forever", err)
	}
	if got := hits.Load(); got != 0 {
		t.Errorf("endpoint hit %d times, want 0", got)
	}
	if sink.count() != 1 {
		t.Fatalf("dead-lettered %d events, want exactly 1", sink.count())
	}
	if reason := sink.reasons()[0]; !strings.Contains(reason, "secret_unopenable") {
		t.Errorf("park reason = %q, want it to name secret_unopenable", reason)
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
		s := senderWithOpener(park, &fakeOpener{err: fmt.Errorf("dial: %w", errs.ErrServiceUnavailable)},
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
		s := senderWithOpener(park, &fakeOpener{err: fmt.Errorf("unwrap: %w", errs.ErrInternal)},
			webhook.WithRetrySink(retry))
		if err := s.Retry(context.Background(), wh, ev, 1, time.Now()); err != nil {
			t.Fatalf("Retry = %v, want nil: this record can never succeed", err)
		}
		if len(retry.calls) != 0 {
			t.Errorf("deferred %d times, want 0: deferring an unopenable secret only delays the same failure",
				len(retry.calls))
		}
		if len(park.calls) != 1 || !strings.Contains(park.calls[0].reason, "secret_unopenable") {
			t.Errorf("park calls = %+v, want one naming secret_unopenable", park.calls)
		}
	})
}
