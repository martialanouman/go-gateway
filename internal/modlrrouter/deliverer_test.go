package modlrrouter_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// --- fakes ---

type fakeLookup struct {
	binds []modlrrouter.LiveBind
	err   error
}

func (f fakeLookup) Lookup(context.Context, uuid.UUID) ([]modlrrouter.LiveBind, error) {
	return f.binds, f.err
}

// fakePod returns a per-bind-id gRPC status, recording the order binds were tried. A bind id absent
// from results delivers (nil).
type fakePod struct {
	results map[string]error
	tried   []string
}

func (f *fakePod) Deliver(_ context.Context, _ string, bindID string, _ []byte) error {
	f.tried = append(f.tried, bindID)
	return f.results[bindID]
}

type fakeWebhookResolver struct {
	wh    cp.Webhook
	found bool
	err   error
}

func (f fakeWebhookResolver) Get(context.Context, uuid.UUID, cp.WebhookEventType) (cp.Webhook, bool, error) {
	return f.wh, f.found, f.err
}

type fakeSender struct {
	sent []webhook.Event
	err  error
}

func (f *fakeSender) Send(_ context.Context, _ cp.Webhook, ev webhook.Event) error {
	f.sent = append(f.sent, ev)
	return f.err
}

type fakeDeliveryMetric struct{ calls []string }

func (f *fakeDeliveryMetric) UndeliveredInc(eventType, reason string) {
	f.calls = append(f.calls, eventType+"/"+reason)
}

func testDelivery() modlrrouter.Delivery {
	return modlrrouter.Delivery{
		AccountID:    uuid.New(),
		EventType:    cp.WebhookEventMO,
		PDU:          []byte("pdu"),
		WebhookEvent: webhook.Event{ID: "ev-1", Payload: []byte(`{}`)},
		DeadLetter:   kafka.Record{Topic: kafka.TopicMODeadLetter, Key: []byte("k"), Value: []byte("v")},
	}
}

func activeWebhook() cp.Webhook {
	return cp.Webhook{ID: uuid.New(), URL: "https://example.test/hook", Status: cp.WebhookActive}
}

// --- tests ---

func TestDelivererDeliversToALiveBind(t *testing.T) {
	pod := &fakePod{}
	prod := &fakeProducer{}
	sender := &fakeSender{}
	metric := &fakeDeliveryMetric{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{binds: []modlrrouter.LiveBind{{PodID: "p1", BindID: "b1"}, {PodID: "p2", BindID: "b2"}}},
		Pods:     pod,
		Webhooks: fakeWebhookResolver{},
		Sender:   sender,
		Producer: prod,
		Metric:   metric,
	})

	if err := dv.Deliver(context.Background(), testDelivery()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(pod.tried) != 1 {
		t.Fatalf("first live bind accepts, only one attempt expected, tried = %v", pod.tried)
	}
	if len(sender.sent) != 0 || len(prod.recs) != 0 || len(metric.calls) != 0 {
		t.Fatalf("delivered bind must skip webhook/dead-letter/metric: sent=%d parked=%d metric=%v", len(sender.sent), len(prod.recs), metric.calls)
	}
}

// TestDelivererRoundRobinsAcrossBinds proves the rotation: over two deliveries to a two-bind account,
// each bind is used once rather than the first bind twice.
func TestDelivererRoundRobinsAcrossBinds(t *testing.T) {
	pod := &fakePod{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{binds: []modlrrouter.LiveBind{{PodID: "p1", BindID: "b1"}, {PodID: "p2", BindID: "b2"}}},
		Pods:     pod,
		Webhooks: fakeWebhookResolver{},
		Sender:   &fakeSender{},
		Producer: &fakeProducer{},
	})

	for range 2 {
		if err := dv.Deliver(context.Background(), testDelivery()); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	seen := map[string]int{}
	for _, b := range pod.tried {
		seen[b]++
	}
	if seen["b1"] != 1 || seen["b2"] != 1 {
		t.Fatalf("round-robin over 2 deliveries = %v, want each bind exactly once", seen)
	}
}

// TestDelivererSkipsTransmitterToNextBind: a FailedPrecondition (a transmitter) on one bind does not
// stop delivery — the round-robin walks on to a receiver. All-but-one bind refuse, so a skip is
// exercised whatever the rotation start.
func TestDelivererSkipsTransmitterToNextBind(t *testing.T) {
	pod := &fakePod{results: map[string]error{
		"b1": status.Error(codes.FailedPrecondition, "transmitter"),
		"b2": status.Error(codes.FailedPrecondition, "transmitter"),
		// b3 is a receiver: it accepts.
	}}
	prod := &fakeProducer{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup: fakeLookup{binds: []modlrrouter.LiveBind{
			{PodID: "p1", BindID: "b1"}, {PodID: "p2", BindID: "b2"}, {PodID: "p3", BindID: "b3"},
		}},
		Pods:     pod,
		Webhooks: fakeWebhookResolver{},
		Sender:   &fakeSender{},
		Producer: prod,
	})

	if err := dv.Deliver(context.Background(), testDelivery()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(pod.tried) < 2 {
		t.Fatalf("a transmitter must be skipped to reach the receiver, tried = %v", pod.tried)
	}
	if pod.tried[len(pod.tried)-1] != "b3" {
		t.Fatalf("delivery must land on the receiver b3, tried = %v", pod.tried)
	}
	if len(prod.recs) != 0 {
		t.Fatalf("a receiver delivered, must not dead-letter")
	}
}

// TestDelivererMalformedPDUFallsBack is the poison-pill guard: an InvalidArgument (a deliver_sm we
// mis-encoded — our own deterministic bug) must NOT crash-loop the consumer. It stops the bind walk
// without an error and falls through to the webhook, else the dead-letter — never a returned error the
// consumer would replay forever.
func TestDelivererMalformedPDUFallsBack(t *testing.T) {
	pod := &fakePod{results: map[string]error{
		"b1": status.Error(codes.InvalidArgument, "malformed pdu"),
		"b2": status.Error(codes.InvalidArgument, "malformed pdu"),
	}}
	prod := &fakeProducer{}
	metric := &fakeDeliveryMetric{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{binds: []modlrrouter.LiveBind{{PodID: "p1", BindID: "b1"}, {PodID: "p2", BindID: "b2"}}},
		Pods:     pod,
		Webhooks: fakeWebhookResolver{found: false}, // no webhook → dead-letter
		Sender:   &fakeSender{},
		Producer: prod,
		Metric:   metric,
	})

	if err := dv.Deliver(context.Background(), testDelivery()); err != nil {
		t.Fatalf("malformed PDU must not return an error the consumer replays: %v", err)
	}
	if len(pod.tried) != 1 {
		t.Fatalf("a malformed PDU is identical for every bind: stop after the first, tried = %v", pod.tried)
	}
	if len(prod.recs) != 1 {
		t.Fatalf("undeliverable event must be dead-lettered, parked %d", len(prod.recs))
	}
}

func TestDelivererFallsBackToWebhook(t *testing.T) {
	prod := &fakeProducer{}
	sender := &fakeSender{}
	metric := &fakeDeliveryMetric{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{}, // no live binds
		Pods:     &fakePod{},
		Webhooks: fakeWebhookResolver{wh: activeWebhook(), found: true},
		Sender:   sender,
		Producer: prod,
		Metric:   metric,
	})

	if err := dv.Deliver(context.Background(), testDelivery()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(sender.sent) != 1 || sender.sent[0].ID != "ev-1" {
		t.Fatalf("expected one webhook send of ev-1, got %v", sender.sent)
	}
	if len(prod.recs) != 0 || len(metric.calls) != 0 {
		t.Fatal("active webhook must not dead-letter")
	}
}

func TestDelivererDeadLettersWhenNoBind(t *testing.T) {
	prod := &fakeProducer{}
	metric := &fakeDeliveryMetric{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{}, // no binds
		Pods:     &fakePod{},
		Webhooks: fakeWebhookResolver{found: false},
		Sender:   &fakeSender{},
		Producer: prod,
		Metric:   metric,
	})

	if err := dv.Deliver(context.Background(), testDelivery()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(prod.recs) != 1 || prod.recs[0].Topic != kafka.TopicMODeadLetter {
		t.Fatalf("expected one dead-letter to %s, got %v", kafka.TopicMODeadLetter, prod.recs)
	}
	if want := "mo/no_bind"; len(metric.calls) != 1 || metric.calls[0] != want {
		t.Fatalf("metric = %v, want [%s]", metric.calls, want)
	}
}

func TestDelivererDeadLettersWhenBindsExhausted(t *testing.T) {
	pod := &fakePod{results: map[string]error{
		"b1": status.Error(codes.Unavailable, "gone"),
		"b2": status.Error(codes.FailedPrecondition, "transmitter"),
	}}
	prod := &fakeProducer{}
	metric := &fakeDeliveryMetric{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{binds: []modlrrouter.LiveBind{{PodID: "p1", BindID: "b1"}, {PodID: "p2", BindID: "b2"}}},
		Pods:     pod,
		Webhooks: fakeWebhookResolver{found: false},
		Sender:   &fakeSender{},
		Producer: prod,
		Metric:   metric,
	})

	if err := dv.Deliver(context.Background(), testDelivery()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(prod.recs) != 1 {
		t.Fatalf("expected one dead-letter, got %d", len(prod.recs))
	}
	if want := "mo/bind_exhausted"; len(metric.calls) != 1 || metric.calls[0] != want {
		t.Fatalf("metric = %v, want [%s]", metric.calls, want)
	}
}

func TestDelivererDeadLettersWhenWebhookDisabled(t *testing.T) {
	wh := activeWebhook()
	wh.Status = cp.WebhookDisabled
	prod := &fakeProducer{}
	sender := &fakeSender{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{},
		Pods:     &fakePod{},
		Webhooks: fakeWebhookResolver{wh: wh, found: true},
		Sender:   sender,
		Producer: prod,
		Metric:   &fakeDeliveryMetric{},
	})

	if err := dv.Deliver(context.Background(), testDelivery()); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(sender.sent) != 0 {
		t.Fatal("disabled webhook must not be sent to")
	}
	if len(prod.recs) != 1 {
		t.Fatalf("disabled webhook must dead-letter, got %d parked", len(prod.recs))
	}
}

// TestDelivererWebhookFallbackIsSigned is the return leg's webhook branch end to end: with no live
// bind, the deliverer hands the event to a REAL webhook.Sender, which POSTs the exact payload to the
// account's endpoint under a verifiable HMAC-SHA256 signature (step-047 construction).
func TestDelivererWebhookFallbackIsSigned(t *testing.T) {
	const secret = "whsec_test"
	payload := []byte(`{"event_id":"ev-1","from":"22507000001"}`)

	type received struct {
		body []byte
		ts   string
		sig  string
	}
	got := make(chan received, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- received{body: b, ts: r.Header.Get(webhook.HeaderTimestamp), sig: r.Header.Get(webhook.HeaderSignature)}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	wh := cp.Webhook{ID: uuid.New(), URL: srv.URL, Secret: secret, EventType: cp.WebhookEventMO, Status: cp.WebhookActive}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{}, // no bind → webhook branch
		Pods:     &fakePod{},
		Webhooks: fakeWebhookResolver{wh: wh, found: true},
		Sender:   webhook.NewSender(srv.Client(), nil, nil),
		Producer: &fakeProducer{},
		Metric:   &fakeDeliveryMetric{},
	})

	d := testDelivery()
	d.WebhookEvent = webhook.Event{ID: "ev-1", Payload: payload}
	if err := dv.Deliver(context.Background(), d); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	r := <-got
	if string(r.body) != string(payload) {
		t.Fatalf("webhook body = %q, want %q", r.body, payload)
	}
	want := "sha256=" + webhook.Sign(secret, r.ts, payload)
	if r.sig != want {
		t.Fatalf("signature = %q, want %q", r.sig, want)
	}
}

// TestDelivererDeadLetterRawParksAndCounts pins the "never silently lost" contract for a deterministic
// build failure: the raw record is parked and the drop is counted.
func TestDelivererDeadLetterRawParksAndCounts(t *testing.T) {
	prod := &fakeProducer{}
	metric := &fakeDeliveryMetric{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup: fakeLookup{}, Pods: &fakePod{}, Webhooks: fakeWebhookResolver{},
		Sender: &fakeSender{}, Producer: prod, Metric: metric,
	})

	rec := kafka.Record{Topic: kafka.TopicDLRDeadLetter, Key: []byte("k"), Value: []byte("v")}
	if err := dv.DeadLetterRaw(context.Background(), rec, "dlr", "encode_error"); err != nil {
		t.Fatalf("DeadLetterRaw: %v", err)
	}
	if len(prod.recs) != 1 || prod.recs[0].Topic != kafka.TopicDLRDeadLetter {
		t.Fatalf("expected one park to %s, got %v", kafka.TopicDLRDeadLetter, prod.recs)
	}
	if want := "dlr/encode_error"; len(metric.calls) != 1 || metric.calls[0] != want {
		t.Fatalf("metric = %v, want [%s]", metric.calls, want)
	}
}

func TestDelivererLookupErrorIsRetryable(t *testing.T) {
	prod := &fakeProducer{}
	dv := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   fakeLookup{err: errors.New("registry down")},
		Pods:     &fakePod{},
		Webhooks: fakeWebhookResolver{},
		Sender:   &fakeSender{},
		Producer: prod,
	})

	if err := dv.Deliver(context.Background(), testDelivery()); err == nil {
		t.Fatal("expected error when the registry lookup fails")
	}
	if len(prod.recs) != 0 {
		t.Fatal("a transient lookup failure must not dead-letter")
	}
}
