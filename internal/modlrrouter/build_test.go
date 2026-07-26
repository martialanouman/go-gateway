package modlrrouter

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

func decodeDeliverSM(t *testing.T, raw []byte) *smpp.DeliverSM {
	t.Helper()
	pdu, err := smpp.Unmarshal(raw)
	if err != nil {
		t.Fatalf("unmarshal deliver_sm: %v", err)
	}
	ds, ok := pdu.Body.(*smpp.DeliverSM)
	if !ok {
		t.Fatalf("body = %T, want *smpp.DeliverSM", pdu.Body)
	}
	return ds
}

// TestMODeliverSMShortBodyOnWire: a short MO body rides the short_message field, source/dest preserved.
func TestMODeliverSMShortBodyOnWire(t *testing.T) {
	mo := pipeline.MORouted{
		From: "22507000001", To: "36000",
		Body: msg.NewBodyString("hello mo"), Encoding: "GSM7",
	}
	ds := decodeDeliverSM(t, mustMO(t, mo))
	if string(ds.ShortMessage) != "hello mo" {
		t.Errorf("short_message = %q, want %q", ds.ShortMessage, "hello mo")
	}
	if _, ok := ds.TLVs.Get(smpp.TagMessagePayload); ok {
		t.Error("short body must not use message_payload TLV")
	}
	if ds.SourceAddr != "22507000001" || ds.DestinationAddr != "36000" {
		t.Errorf("addrs = %q -> %q, want 22507000001 -> 36000", ds.SourceAddr, ds.DestinationAddr)
	}
}

// TestMODeliverSMLongBodyUsesTLV: a body over 254 octets moves to the message_payload TLV, leaving
// short_message empty (the segmentation-free path for long MO).
func TestMODeliverSMLongBodyUsesTLV(t *testing.T) {
	long := strings.Repeat("x", 300)
	ds := decodeDeliverSM(t, mustMO(t, pipeline.MORouted{Body: msg.NewBodyString(long)}))
	if len(ds.ShortMessage) != 0 {
		t.Errorf("short_message len = %d, want 0 (body in TLV)", len(ds.ShortMessage))
	}
	payload, ok := ds.TLVs.Get(smpp.TagMessagePayload)
	if !ok || string(payload) != long {
		t.Errorf("message_payload TLV = %q (present=%t), want the long body", payload, ok)
	}
}

// TestDLRDeliverSMIsAReceipt: a DLR encodes as a delivery-receipt deliver_sm referencing the account's
// own message id, with no message body.
func TestDLRDeliverSMIsAReceipt(t *testing.T) {
	mid := uuid.New()
	m := dlrmap.Mapping{MessageID: mid, SourceAddr: "36000", DestAddr: "22507000001"}
	dlr := pipeline.DLREvent{Stat: "DELIVRD", State: 2, ErrorCode: "000"}
	raw, err := dlrDeliverSM(m, dlr)
	if err != nil {
		t.Fatalf("dlrDeliverSM: %v", err)
	}
	ds := decodeDeliverSM(t, raw)
	if ds.ESMClass&smpp.ESMClassMCDeliveryReceipt == 0 {
		t.Errorf("esm_class = %#x, want the delivery-receipt bit set", ds.ESMClass)
	}
	if !bytes.Contains(ds.ShortMessage, []byte("id:"+mid.String())) ||
		!bytes.Contains(ds.ShortMessage, []byte("stat:DELIVRD")) {
		t.Errorf("receipt text = %q, want id/stat of the mapping", ds.ShortMessage)
	}
	if rid, ok := ds.TLVs.Get(smpp.TagReceiptedMessageID); !ok || string(rid) != mid.String() {
		t.Errorf("receipted_message_id TLV = %q (present=%t), want %s", rid, ok, mid)
	}
	// A receipt's source is the MSISDN the MT went to; its dest is the original sender id.
	if ds.SourceAddr != "22507000001" || ds.DestinationAddr != "36000" {
		t.Errorf("addrs = %q -> %q, want 22507000001 -> 36000", ds.SourceAddr, ds.DestinationAddr)
	}
}

// TestWebhookBodyEventIDsAreStable pins the dedup keys: an MO's event id is its message id; a DLR's
// includes the state so distinct states of one message are distinct events.
func TestWebhookBodyEventIDsAreStable(t *testing.T) {
	mid := uuid.New()
	moID, _, err := moWebhookBody(pipeline.MORouted{MessageID: mid, Body: msg.NewBodyString("x"), ReceivedAt: time.Unix(1, 0)})
	if err != nil {
		t.Fatalf("moWebhookBody: %v", err)
	}
	if moID != mid.String() {
		t.Errorf("mo event id = %q, want %q", moID, mid)
	}
	dlrID, _, err := dlrWebhookBody(dlrmap.Mapping{MessageID: mid}, pipeline.DLREvent{State: 5})
	if err != nil {
		t.Fatalf("dlrWebhookBody: %v", err)
	}
	if want := mid.String() + ":5"; dlrID != want {
		t.Errorf("dlr event id = %q, want %q", dlrID, want)
	}
}

func mustMO(t *testing.T, mo pipeline.MORouted) []byte {
	t.Helper()
	raw, err := moDeliverSM(mo)
	if err != nil {
		t.Fatalf("moDeliverSM: %v", err)
	}
	return raw
}
