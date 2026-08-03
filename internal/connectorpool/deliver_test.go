package connectorpool

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"

	"github.com/martialanouman/go-gateway/internal/storage/kafka"
)

// TestParseReceiptTextDropsBodyPreview is the invariant-(a) guard: the "text:" field of a receipt
// echoes the first chars of the ORIGINAL message and must never be captured.
func TestParseReceiptTextDropsBodyPreview(t *testing.T) {
	body := "id:abc123 sub:001 dlvrd:001 submit date:2401011200 done date:2401011205 stat:DELIVRD err:000 text:SECRETcontent"
	got := parseReceiptText(body)

	if got["id"] != "abc123" || got["stat"] != "DELIVRD" || got["err"] != "000" {
		t.Errorf("fields = %v, want id/stat/err parsed", got)
	}
	if got["submit date"] != "2401011200" || got["done date"] != "2401011205" {
		t.Errorf("dates = %q/%q, want the two-word keys parsed", got["submit date"], got["done date"])
	}
	if _, ok := got["text"]; ok {
		t.Error("the text: field must not be captured (it echoes the message body)")
	}
	for k, v := range got {
		if strings.Contains(v, "SECRET") {
			t.Errorf("field %q captured body-preview content %q", k, v)
		}
	}
}

// TestParseReceiptPrefersTLVs: the receipted message id and message state come from the TLVs when
// present, with the textual stat/err filling the rest.
func TestParseReceiptPrefersTLVs(t *testing.T) {
	ds := &smpp.DeliverSM{}
	ds.ShortMessage = []byte("id:ignored stat:DELIVRD err:007")
	ds.TLVs.Set(smpp.TagReceiptedMessageID, []byte("smsc-42"))
	ds.TLVs.Set(smpp.TagMessageState, []byte{2})

	r := parseReceipt(ds)
	if r.smscMsgID != "smsc-42" {
		t.Errorf("smscMsgID = %q, want smsc-42 (from TLV, not the text)", r.smscMsgID)
	}
	if r.state != 2 {
		t.Errorf("state = %d, want 2 (from TLV)", r.state)
	}
	if r.stat != "DELIVRD" || r.errCode != "007" {
		t.Errorf("stat/err = %q/%q, want DELIVRD/007 (from text)", r.stat, r.errCode)
	}
}

// TestParseReceiptFallsBackToText: with no TLVs, the id and stat come from the receipt text and state
// stays zero.
func TestParseReceiptFallsBackToText(t *testing.T) {
	ds := &smpp.DeliverSM{}
	ds.ShortMessage = []byte("id:xyz789 stat:EXPIRED")

	r := parseReceipt(ds)
	if r.smscMsgID != "xyz789" {
		t.Errorf("smscMsgID = %q, want xyz789 (from text)", r.smscMsgID)
	}
	if r.stat != "EXPIRED" {
		t.Errorf("stat = %q, want EXPIRED", r.stat)
	}
	if r.state != 0 {
		t.Errorf("state = %d, want 0 (no TLV)", r.state)
	}
}

// TestBuildMOUsesMessagePayloadWhenShortMessageEmpty: a large MO arrives in the message_payload TLV;
// buildMO must read the body from there when short_message is empty.
func TestBuildMOUsesMessagePayloadWhenShortMessageEmpty(t *testing.T) {
	// A producer is required since step-201c (D8), even here: buildMO is pure, but New refuses to build
	// a pool that could send without recording.
	svc := New(Deps{Producer: discardProducer{}})
	ds := &smpp.DeliverSM{}
	ds.SourceAddr, ds.DestinationAddr = "22507000001", "36000"
	ds.TLVs.Set(smpp.TagMessagePayload, []byte("long body from payload"))

	mo := svc.buildMO(ds, time.Now())
	if string(mo.Body.Reveal()) != "long body from payload" {
		t.Errorf("body = %q, want the message_payload content", mo.Body.Reveal())
	}
}

// discardProducer drops every record. It is what a test wires when it asserts nothing about the return
// path — explicitly, so the choice reads in the test rather than being made by the constructor.
type discardProducer struct{}

func (discardProducer) Produce(context.Context, kafka.Record) error { return nil }
