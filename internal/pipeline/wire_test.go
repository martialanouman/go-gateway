package pipeline_test

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

func TestInboundRoundTrip(t *testing.T) {
	const text = "confidential body text"
	validity := "3600"
	clientRef := "ref-42"
	dataCoding := 8
	in := pipeline.InboundMT{
		MessageID:          uuid.New(),
		TraceID:            uuid.New(),
		AccountID:          uuid.New(),
		CustomerID:         uuid.New(),
		From:               "GATEWAY",
		To:                 "+22507000000",
		Body:               msg.NewBodyString(text),
		Encoding:           "auto",
		RegisteredDelivery: true,
		ValidityPeriod:     &validity,
		Priority:           2,
		ClientRef:          &clientRef,
		DataCoding:         &dataCoding,
		SubmittedAt:        time.Now().UTC().Truncate(time.Millisecond),
	}

	rec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if rec.Topic != kafka.TopicMTInbound {
		t.Errorf("topic: got %q", rec.Topic)
	}
	if !bytes.Equal(rec.Key, in.MessageID[:]) {
		t.Errorf("key should be the message id bytes")
	}

	out, err := pipeline.DecodeInbound(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(out.Body.Reveal()) != text {
		t.Errorf("body: got %q want %q", out.Body.Reveal(), text)
	}
	if out.MessageID != in.MessageID || out.TraceID != in.TraceID {
		t.Error("ids not preserved")
	}
	if out.To != in.To || out.From != in.From || out.Encoding != in.Encoding {
		t.Error("fields not preserved")
	}
	if out.ValidityPeriod == nil || *out.ValidityPeriod != validity || out.Priority != 2 {
		t.Error("optional fields not preserved")
	}
	if !out.SubmittedAt.Equal(in.SubmittedAt) {
		t.Errorf("submitted_at: got %s want %s", out.SubmittedAt, in.SubmittedAt)
	}
}

// routedFixture is a minimal valid RoutedMT for the fallback-chain header tests.
func routedFixture() pipeline.RoutedMT {
	return pipeline.RoutedMT{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		From: "GATEWAY", To: "+22507000000", Body: msg.NewBodyString("hi"),
		Encoding: "gsm7", ConnectorID: uuid.New(), SegmentCount: 1, SubmittedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
}

// kafka.NewProducer keeps franz-go's default partitioner, whose keyed branch is the murmur2 hash that
// StickyKeyPartitioner wraps; the default itself panics when called outside a client.
func TestInboundSingleAccountSpreadsOverEveryPartition(t *testing.T) {
	const partitions = 12
	partitioner := kgo.StickyKeyPartitioner(nil).ForTopic(kafka.TopicMTInbound)
	account := uuid.New()
	hit := map[int]bool{}
	for range 20 * partitions {
		rec, err := pipeline.EncodeInbound(pipeline.InboundMT{MessageID: uuid.New(), AccountID: account})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		hit[partitioner.Partition(&kgo.Record{Key: rec.Key}, partitions)] = true
	}
	if len(hit) != partitions {
		t.Errorf("one account reached %d of %d partitions, want all", len(hit), partitions)
	}
}

func TestRoutedRoundTrip(t *testing.T) {
	routeID := uuid.New()
	in := pipeline.RoutedMT{
		MessageID:    uuid.New(),
		TraceID:      uuid.New(),
		AccountID:    uuid.New(),
		CustomerID:   uuid.New(),
		From:         "GATEWAY",
		To:           "+22507000000",
		Body:         msg.NewBodyString("hi"),
		Encoding:     "gsm7",
		ConnectorID:  uuid.New(),
		RouteID:      &routeID,
		SegmentSeq:   2,
		SegmentCount: 3,
		HasUDH:       true,
		SubmittedAt:  time.Now().UTC().Truncate(time.Millisecond),
		Billable:     true,
		OwnerType:    "smpp_account",
	}

	rec, err := pipeline.EncodeRouted(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if rec.Topic != kafka.TopicMTRouted {
		t.Errorf("topic: got %q", rec.Topic)
	}
	// Keyed by the logical message id so every segment reaches the same bind (§7.3).
	if !bytes.Equal(rec.Key, in.MessageID[:]) {
		t.Errorf("key should be the message id bytes")
	}

	out, err := pipeline.DecodeRouted(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.ConnectorID != in.ConnectorID || out.RouteID == nil || *out.RouteID != routeID {
		t.Error("routing fields not preserved")
	}
	if out.SegmentSeq != 2 || out.SegmentCount != 3 || !out.HasUDH || string(out.Body.Reveal()) != "hi" {
		t.Error("body/segment fields not preserved")
	}
	// The billing settlement contract (step-145): connector-pool reads Billable to decide whether to
	// capture at all, and OwnerType to capture against the identical balance key the router reserved.
	if !out.Billable || out.OwnerType != "smpp_account" {
		t.Errorf("(Billable, OwnerType) = (%v, %q), want (true, smpp_account)", out.Billable, out.OwnerType)
	}
}

// TestBodyNeverInHeaders is the invariant (a) guard at the transport boundary: the plaintext lives
// only in the record value (the durable data plane), never in a header.
func TestBodyNeverInHeaders(t *testing.T) {
	const secret = "a very secret message"
	in := pipeline.InboundMT{
		MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: uuid.New(),
		To: "+22507000000", From: "GATEWAY", Body: msg.NewBodyString(secret),
		Encoding: "gsm7", SubmittedAt: time.Now().UTC(),
	}
	rec, err := pipeline.EncodeInbound(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	for _, h := range rec.Headers {
		if strings.Contains(string(h.Value), secret) {
			t.Fatalf("header %q leaked the body", h.Key)
		}
	}
	// The value carries it (that is allowed — it is the durable payload, not a log or a header). In
	// JSON a []byte is base64-encoded, so the encoded body must appear in the value.
	encoded := base64.StdEncoding.EncodeToString([]byte(secret))
	if !bytes.Contains(rec.Value, []byte(encoded)) {
		t.Error("expected the body to be present (base64) in the record value")
	}

	// The masked in-process form never shows the plaintext.
	if strings.Contains(in.Body.String(), secret) {
		t.Error("msg.Body.String() must not reveal the plaintext")
	}
}

func TestRoutedFallbackChainHeaderRoundTrips(t *testing.T) {
	c1, c2, c3 := uuid.New(), uuid.New(), uuid.New()
	env := routedFixture()
	env.FallbackChain = []uuid.UUID{c1, c2, c3}
	rec, err := pipeline.EncodeRouted(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// The chain travels in the header, never the value/body.
	if _, ok := rec.Header(kafka.HeaderFallbackChain); !ok {
		t.Fatal("fallback_chain header missing")
	}
	got, err := pipeline.DecodeRouted(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.FallbackChain) != 3 || got.FallbackChain[0] != c1 || got.FallbackChain[2] != c3 {
		t.Errorf("chain = %v, want [%s %s %s]", got.FallbackChain, c1, c2, c3)
	}
}

func TestRoutedNoChainHeaderWhenEmpty(t *testing.T) {
	rec, err := pipeline.EncodeRouted(routedFixture())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, ok := rec.Header(kafka.HeaderFallbackChain); ok {
		t.Error("empty chain must not emit a fallback_chain header")
	}
	got, _ := pipeline.DecodeRouted(rec)
	if got.FallbackChain != nil {
		t.Errorf("chain = %v, want nil", got.FallbackChain)
	}
}

func TestDecodeChainToleratesMalformed(t *testing.T) {
	good := uuid.New()
	rec, err := pipeline.EncodeRouted(routedFixture())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Hand-craft a messy header: a bad token, a duplicate, whitespace.
	rec.Headers = append(rec.Headers, kafka.Header{
		Key:   kafka.HeaderFallbackChain,
		Value: []byte("not-a-uuid, " + good.String() + " ," + good.String()),
	})
	got, _ := pipeline.DecodeRouted(rec)
	if len(got.FallbackChain) != 1 || got.FallbackChain[0] != good {
		t.Errorf("chain = %v, want [%s] (bad skipped, dup dropped)", got.FallbackChain, good)
	}
}

// The category and effective priority must survive every republish of mt.routed (reroute, parking,
// replay), and the pool reads them by their JSON names: assert the bytes, not only a round trip.
func TestRoutedCarriesCategoryAndPriority(t *testing.T) {
	in := routedFixture()
	in.TrafficCategory = cp.TrafficTransactional
	in.Priority = 2
	rec, err := pipeline.EncodeRouted(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, want := range []string{`"traffic_category":"transactional"`, `"priority":2`} {
		if !strings.Contains(string(rec.Value), want) {
			t.Errorf("mt.routed value lacks %s: %s", want, rec.Value)
		}
	}
	out, err := pipeline.DecodeRouted(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.TrafficCategory != cp.TrafficTransactional || out.Priority != 2 {
		t.Errorf("decoded category/priority = %q/%d, want transactional/2", out.TrafficCategory, out.Priority)
	}
}

// A record produced before the fields existed decodes as marketing, priority 0: the most constrained.
func TestRoutedWithoutCategoryDecodesAsMarketing(t *testing.T) {
	rec, err := pipeline.EncodeRouted(routedFixture())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec.Value = []byte(strings.NewReplacer(`"traffic_category":"",`, "", `"priority":0,`, "").Replace(string(rec.Value)))
	if strings.Contains(string(rec.Value), "traffic_category") {
		t.Fatalf("fixture still carries the field: %s", rec.Value)
	}
	out, err := pipeline.DecodeRouted(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.TrafficCategory != cp.TrafficMarketing || out.Priority != 0 {
		t.Errorf("legacy record decoded as %q/%d, want marketing/0", out.TrafficCategory, out.Priority)
	}
}

// mt.outcome crosses services deployed one after the other: pin the field names, not only a round trip.
func TestOutcomeCarriesCategoryAndPriority(t *testing.T) {
	rec, err := pipeline.EncodeOutcome(pipeline.OutcomeMT{MessageID: uuid.New(), TrafficCategory: "transactional", Priority: 2})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, want := range []string{`"traffic_category":"transactional"`, `"priority":2`} {
		if !strings.Contains(string(rec.Value), want) {
			t.Errorf("mt.outcome value lacks %s: %s", want, rec.Value)
		}
	}
}

// billing-svc settles from mt.outcome (step-287d): it needs to know a reservation exists and against which
// balance, under the field names it decodes.
func TestOutcomeCarriesTheReservation(t *testing.T) {
	rec, err := pipeline.EncodeOutcome(pipeline.OutcomeMT{MessageID: uuid.New(), Billable: true, OwnerType: "smpp_account"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	for _, want := range []string{`"billable":true`, `"owner_type":"smpp_account"`} {
		if !strings.Contains(string(rec.Value), want) {
			t.Errorf("mt.outcome value lacks %s: %s", want, rec.Value)
		}
	}
}
