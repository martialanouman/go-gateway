package smpp

import (
	"bytes"
	"testing"
)

func TestTLVListGetAndOrder(t *testing.T) {
	var l TLVList
	l.Set(TagReceiptedMessageID, []byte("id-1"))
	l.Set(0x1400, []byte("vendor"))
	l.Set(TagReceiptedMessageID, []byte("id-2")) // repeated tag: Get returns the first

	if v, ok := l.Get(TagReceiptedMessageID); !ok || string(v) != "id-1" {
		t.Errorf("Get first: got %q ok=%v", v, ok)
	}
	if v, ok := l.Get(0x1400); !ok || string(v) != "vendor" {
		t.Errorf("Get vendor: got %q ok=%v", v, ok)
	}
	if _, ok := l.Get(0x9999); ok {
		t.Error("Get absent tag: expected ok=false")
	}
	if len(l) != 3 {
		t.Errorf("Set should append without dedup: len=%d", len(l))
	}
}

func TestTLVMarshalRoundTripInResp(t *testing.T) {
	want := TLVList{
		{Tag: TagSCInterfaceVersion, Value: []byte{0x34}},
		{Tag: 0x1400, Value: []byte{}},
		{Tag: TagReceiptedMessageID, Value: []byte("smsc-0001")},
	}
	b, err := Marshal(PDU{Sequence: 1, Body: &SubmitSMResp{messageIDResp{MessageID: "m", TLVs: want}}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	tlvs := got.Body.(*SubmitSMResp).TLVs
	if len(tlvs) != len(want) {
		t.Fatalf("TLV count: got %d want %d", len(tlvs), len(want))
	}
	for i := range want {
		if tlvs[i].Tag != want[i].Tag || !bytes.Equal(tlvs[i].Value, want[i].Value) {
			t.Errorf("TLV[%d]: got {%#x %x} want {%#x %x}", i, tlvs[i].Tag, tlvs[i].Value, want[i].Tag, want[i].Value)
		}
	}
}
