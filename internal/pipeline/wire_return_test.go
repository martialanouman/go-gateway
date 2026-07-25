package pipeline_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// TestMORoundTrip: an MOInbound survives Encode -> Decode intact, including the masked body, and is
// keyed and topic-tagged for mo.inbound.
func TestMORoundTrip(t *testing.T) {
	const text = "mobile originated secret"
	env := pipeline.MOInbound{
		ConnectorID: uuid.New(),
		From:        "22507000001",
		To:          "36000",
		Body:        msg.NewBodyString(text),
		DataCoding:  8,
		ESMClass:    0,
		ReceivedAt:  time.Now().UTC().Truncate(time.Second),
	}

	rec, err := pipeline.EncodeMO(env)
	if err != nil {
		t.Fatalf("EncodeMO: %v", err)
	}
	if rec.Topic != kafka.TopicMOInbound {
		t.Errorf("topic = %q, want %q", rec.Topic, kafka.TopicMOInbound)
	}
	if string(rec.Key) != env.To {
		t.Errorf("key = %q, want the inbound number %q", rec.Key, env.To)
	}

	got, err := pipeline.DecodeMO(rec)
	if err != nil {
		t.Fatalf("DecodeMO: %v", err)
	}
	if got.ConnectorID != env.ConnectorID || got.From != env.From || got.To != env.To ||
		got.DataCoding != env.DataCoding || !got.ReceivedAt.Equal(env.ReceivedAt) {
		t.Errorf("round trip mismatch:\n got  %+v\n want %+v", got, env)
	}
	if string(got.Body.Reveal()) != text {
		t.Errorf("body = %q, want %q", got.Body.Reveal(), text)
	}
}

// TestMOBodyIsMaskedInHeadersNotValue: the plaintext travels in the record VALUE (the durable data
// plane) but never leaks through a masked Body's default rendering — the wire uses the revealed bytes
// only inside the audited codec.
func TestMOBodyMaskedRendering(t *testing.T) {
	const text = "confidential mo"
	env := pipeline.MOInbound{ConnectorID: uuid.New(), From: "1", To: "2", Body: msg.NewBodyString(text)}

	// The masked Body must never render the plaintext (log/span safety).
	if strings.Contains(env.Body.String(), text) {
		t.Fatal("msg.Body.String must not reveal the plaintext")
	}
	// It is present, base64-encoded, only in the record value.
	rec, err := pipeline.EncodeMO(env)
	if err != nil {
		t.Fatalf("EncodeMO: %v", err)
	}
	if !bytes.Contains(rec.Value, []byte("Y29uZmlkZW50aWFsIG1v")) { // base64("confidential mo")
		t.Error("the revealed body should be the base64 value in the durable record")
	}
}

// TestMORoutedRoundTrip: an MORouted survives Encode -> Decode intact, including the masked body, and
// is keyed by the resolved account for mo.routed.
func TestMORoutedRoundTrip(t *testing.T) {
	const text = "routed mo body"
	env := pipeline.MORouted{
		MessageID:       uuid.New(),
		TraceID:         uuid.New(),
		AccountID:       uuid.New(),
		CustomerID:      uuid.New(),
		InboundNumberID: uuid.New(),
		ConnectorID:     uuid.New(),
		From:            "22507000001",
		To:              "36000",
		Body:            msg.NewBodyString(text),
		DataCoding:      0,
		Encoding:        "gsm7",
		ReceivedAt:      time.Now().UTC().Truncate(time.Second),
	}

	rec, err := pipeline.EncodeMORouted(env)
	if err != nil {
		t.Fatalf("EncodeMORouted: %v", err)
	}
	if rec.Topic != kafka.TopicMORouted {
		t.Errorf("topic = %q, want %q", rec.Topic, kafka.TopicMORouted)
	}
	if key := env.AccountID; string(rec.Key) != string(key[:]) {
		t.Errorf("key = %x, want the account id %x", rec.Key, key[:])
	}
	if strings.Contains(string(rec.Value), text) {
		// the plaintext must be base64 in the value, never raw.
		t.Error("body appears raw in the record value; it must be base64")
	}

	got, err := pipeline.DecodeMORouted(rec)
	if err != nil {
		t.Fatalf("DecodeMORouted: %v", err)
	}
	if got.MessageID != env.MessageID || got.AccountID != env.AccountID || got.CustomerID != env.CustomerID ||
		got.InboundNumberID != env.InboundNumberID || got.ConnectorID != env.ConnectorID ||
		got.From != env.From || got.To != env.To || got.Encoding != env.Encoding ||
		!got.ReceivedAt.Equal(env.ReceivedAt) {
		t.Errorf("round trip mismatch:\n got  %+v\n want %+v", got, env)
	}
	if string(got.Body.Reveal()) != text {
		t.Errorf("body = %q, want %q", got.Body.Reveal(), text)
	}
}

// TestDLRRoundTrip: a DLREvent survives Encode -> Decode, keyed by the SMSC message id for dlr.events.
func TestDLRRoundTrip(t *testing.T) {
	env := pipeline.DLREvent{
		ConnectorID:   uuid.New(),
		SMSCMessageID: "00000000000000ab",
		State:         2,
		Stat:          "DELIVRD",
		ErrorCode:     "000",
		SubmitDate:    "2401011200",
		DoneDate:      "2401011205",
		ReceivedAt:    time.Now().UTC().Truncate(time.Second),
	}

	rec, err := pipeline.EncodeDLR(env)
	if err != nil {
		t.Fatalf("EncodeDLR: %v", err)
	}
	if rec.Topic != kafka.TopicDLREvents {
		t.Errorf("topic = %q, want %q", rec.Topic, kafka.TopicDLREvents)
	}
	if string(rec.Key) != env.SMSCMessageID {
		t.Errorf("key = %q, want the smsc message id %q", rec.Key, env.SMSCMessageID)
	}

	got, err := pipeline.DecodeDLR(rec)
	if err != nil {
		t.Fatalf("DecodeDLR: %v", err)
	}
	if got.ConnectorID != env.ConnectorID || got.SMSCMessageID != env.SMSCMessageID ||
		got.State != env.State || got.Stat != env.Stat || got.ErrorCode != env.ErrorCode ||
		got.SubmitDate != env.SubmitDate || got.DoneDate != env.DoneDate ||
		!got.ReceivedAt.Equal(env.ReceivedAt) {
		t.Errorf("round trip mismatch:\n got  %+v\n want %+v", got, env)
	}
}
