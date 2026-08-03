package pipeline_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// outcomeFixture is a fully-populated OutcomeMT: every optional field is set, so a round trip that
// silently drops one is caught rather than passing on zero values.
func outcomeFixture() pipeline.OutcomeMT {
	routeID := uuid.New()
	code := "submit_failed"
	charged := int32(3)
	return pipeline.OutcomeMT{
		MessageID:      uuid.New(),
		TraceID:        uuid.New(),
		AccountID:      uuid.New(),
		CustomerID:     uuid.New(),
		ConnectorID:    uuid.New(),
		RouteID:        &routeID,
		From:           "GATEWAY",
		To:             "+22507000000",
		Encoding:       "ucs2",
		SegmentSeq:     2,
		SegmentCount:   3,
		SubmittedAt:    time.Now().UTC().Truncate(time.Millisecond),
		Status:         "failed",
		ErrorCode:      &code,
		Billed:         true,
		CreditsCharged: &charged,
	}
}

// TestOutcomeRoundTrip: everything the CDR projection needs to rebuild the row the connector used to
// write itself survives the wire, and the record is keyed by the logical message id so a message's
// segments and successive outcomes stay on one partition, in submit order (§7.3).
func TestOutcomeRoundTrip(t *testing.T) {
	in := outcomeFixture()

	rec, err := pipeline.EncodeOutcome(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if rec.Topic != kafka.TopicMTOutcome {
		t.Errorf("topic = %q, want %q", rec.Topic, kafka.TopicMTOutcome)
	}
	if !bytes.Equal(rec.Key, in.MessageID[:]) {
		t.Errorf("key = %x, want the message id bytes %x", rec.Key, in.MessageID[:])
	}
	if _, ok := rec.Header(kafka.HeaderMessageID); !ok {
		t.Error("the id headers every pipeline record carries are missing")
	}

	out, err := pipeline.DecodeOutcome(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.MessageID != in.MessageID || out.TraceID != in.TraceID ||
		out.AccountID != in.AccountID || out.CustomerID != in.CustomerID {
		t.Errorf("ids = %+v, want %+v", out, in)
	}
	if out.ConnectorID != in.ConnectorID || out.RouteID == nil || *out.RouteID != *in.RouteID {
		t.Errorf("routing = (%v, %v), want (%v, %v)", out.ConnectorID, out.RouteID, in.ConnectorID, in.RouteID)
	}
	if out.From != in.From || out.To != in.To || out.Encoding != in.Encoding {
		t.Errorf("addressing = (%q, %q, %q), want (%q, %q, %q)",
			out.From, out.To, out.Encoding, in.From, in.To, in.Encoding)
	}
	if out.SegmentSeq != in.SegmentSeq || out.SegmentCount != in.SegmentCount {
		t.Errorf("segments = %d/%d, want %d/%d", out.SegmentSeq, out.SegmentCount, in.SegmentSeq, in.SegmentCount)
	}
	if !out.SubmittedAt.Equal(in.SubmittedAt) {
		t.Errorf("submitted_at = %s, want %s", out.SubmittedAt, in.SubmittedAt)
	}
	if out.Status != in.Status || out.ErrorCode == nil || *out.ErrorCode != *in.ErrorCode {
		t.Errorf("outcome = (%q, %v), want (%q, %v)", out.Status, out.ErrorCode, in.Status, in.ErrorCode)
	}
	if !out.Billed || out.CreditsCharged == nil || *out.CreditsCharged != *in.CreditsCharged {
		t.Errorf("billing = (%v, %v), want (%v, %v)",
			out.Billed, out.CreditsCharged, in.Billed, in.CreditsCharged)
	}
}

// TestOutcomeOmitsNilOptionals: a nil route id / error code / capture result decodes back to nil rather
// than to a zero uuid or an empty-string code, which the CDR would store as a real value.
func TestOutcomeOmitsNilOptionals(t *testing.T) {
	in := outcomeFixture()
	in.RouteID, in.ErrorCode, in.CreditsCharged, in.Billed = nil, nil, nil, false
	in.Status = "enroute"

	rec, err := pipeline.EncodeOutcome(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := pipeline.DecodeOutcome(rec)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.RouteID != nil || out.ErrorCode != nil || out.CreditsCharged != nil || out.Billed {
		t.Errorf("optionals = (route %v, code %v, charged %v, billed %v), want all nil/false",
			out.RouteID, out.ErrorCode, out.CreditsCharged, out.Billed)
	}
}

// TestOutcomeWireCarriesNoBody is invariant (a) at the codec: the enroute/failed CDR row stores no
// content (only the accepted row does, sealed by the ingest projection), so the outcome event has no
// reason to carry a body — and a field added later would silently ship plaintext to a topic whose
// consumer writes it nowhere. The check is on the serialised value, not on the Go struct, so a body
// smuggled under any key name fails here.
func TestOutcomeWireCarriesNoBody(t *testing.T) {
	rec, err := pipeline.EncodeOutcome(outcomeFixture())
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(rec.Value, &fields); err != nil {
		t.Fatalf("unmarshal wire value: %v", err)
	}
	if len(fields) == 0 {
		t.Fatal("the wire value decoded to no fields at all — the check would be vacuous")
	}
	for name := range fields {
		switch name {
		case "body", "content", "short_message", "message_payload", "text":
			t.Errorf("mt.outcome carries a %q field: the CDR outcome row stores no content (invariant a)", name)
		}
	}
}
