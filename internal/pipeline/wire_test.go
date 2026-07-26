package pipeline_test

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

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
	// Keyed by account so an account's submissions keep order (§1.6).
	if !bytes.Equal(rec.Key, in.AccountID[:]) {
		t.Errorf("key should be the account id bytes")
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
