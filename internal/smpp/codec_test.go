package smpp

import (
	"bytes"
	"testing"
)

// samplePDUs returns one representative PDU per body type, exercised by the round-trip test and
// reused as fuzz seeds.
func samplePDUs() []PDU {
	return []PDU{
		{Sequence: 1, Body: &BindTransmitter{BindFields{
			SystemID: "esme01", Password: "s3cret", SystemType: "SMPP",
			InterfaceVersion: InterfaceVersion34, AddrTON: TONInternational, AddrNPI: NPIISDN,
		}}},
		{Sequence: 2, Body: &BindReceiver{BindFields{SystemID: "esme01", Password: "s3cret"}}},
		{Sequence: 3, Body: &BindTransceiver{BindFields{SystemID: "esme01", Password: "s3cret"}}},
		{Sequence: 1, Body: &BindTransmitterResp{BindRespFields{SystemID: "SMSC-A",
			TLVs: TLVList{{Tag: TagSCInterfaceVersion, Value: []byte{0x34}}}}}},
		{Sequence: 2, Body: &BindReceiverResp{BindRespFields{SystemID: "SMSC-A"}}},
		{Sequence: 3, Body: &BindTransceiverResp{BindRespFields{SystemID: "SMSC-A"}}},
		{Sequence: 10, Body: &SubmitSM{SMFields{
			SourceAddrTON: TONAlphanumeric, SourceAddr: "GATEWAY",
			DestAddrTON: TONInternational, DestAddrNPI: NPIISDN, DestinationAddr: "22507000000",
			ESMClass: ESMClassDefault, RegisteredDelivery: RegisteredDeliveryReceipt,
			DataCoding: DataCodingGSM7, ShortMessage: []byte("hello world"),
		}}},
		{Sequence: 10, Body: &SubmitSMResp{MessageIDResp{MessageID: "smsc-msg-0001"}}},
		{Sequence: 11, Body: &DeliverSM{SMFields{
			SourceAddr: "22507000000", DestinationAddr: "GATEWAY",
			ESMClass: ESMClassMCDeliveryReceipt, ShortMessage: []byte("id:smsc-msg-0001 stat:DELIVRD"),
			TLVs: TLVList{{Tag: TagReceiptedMessageID, Value: []byte("smsc-msg-0001")}},
		}}},
		{Sequence: 11, Body: &DeliverSMResp{MessageIDResp{}}},
		{Sequence: 20, Body: &EnquireLink{}},
		{Sequence: 20, Body: &EnquireLinkResp{}},
		{Sequence: 30, Body: &Unbind{}},
		{Sequence: 30, Body: &UnbindResp{}},
		{Sequence: 0, Status: 0x08, Body: &GenericNACK{}},
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	for _, want := range samplePDUs() {
		want := want
		t.Run(commandName(want.CommandID()), func(t *testing.T) {
			b, err := Marshal(want)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			got, err := Unmarshal(b)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			// Re-marshalling the decoded PDU must reproduce the original bytes exactly: the codec is
			// canonical, so a round-trip loses nothing.
			b2, err := Marshal(got)
			if err != nil {
				t.Fatalf("re-Marshal: %v", err)
			}
			if !bytes.Equal(b, b2) {
				t.Fatalf("round-trip mismatch\n first: %x\nsecond: %x", b, b2)
			}
			if got.CommandID() != want.CommandID() {
				t.Fatalf("command id: got %#x want %#x", got.CommandID(), want.CommandID())
			}
			if got.Sequence != want.Sequence || got.Status != want.Status {
				t.Fatalf("header: got seq=%d status=%#x want seq=%d status=%#x",
					got.Sequence, got.Status, want.Sequence, want.Status)
			}
		})
	}
}

func TestSubmitSMFieldsPreserved(t *testing.T) {
	want := &SubmitSM{SMFields{
		ServiceType: "WAP", SourceAddr: "GATEWAY", DestinationAddr: "22507000000",
		ESMClass: ESMClassUDHIndicator, DataCoding: DataCodingUCS2,
		ShortMessage: []byte{0x01, 0x02, 0x00, 0x03}, // includes a NUL: proves octet-string handling
		TLVs:         TLVList{{Tag: 0x1400, Value: []byte("vendor")}},
	}}
	b, err := Marshal(PDU{Sequence: 7, Body: want})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	sm, ok := got.Body.(*SubmitSM)
	if !ok {
		t.Fatalf("body type: got %T", got.Body)
	}
	if !bytes.Equal(sm.ShortMessage, want.ShortMessage) {
		t.Errorf("ShortMessage: got %x want %x", sm.ShortMessage, want.ShortMessage)
	}
	if sm.DataCoding != DataCodingUCS2 || sm.ESMClass != ESMClassUDHIndicator {
		t.Errorf("coding/esm: got dc=%#x esm=%#x", sm.DataCoding, sm.ESMClass)
	}
	if v, ok := sm.TLVs.Get(0x1400); !ok || string(v) != "vendor" {
		t.Errorf("vendor TLV not preserved: %q ok=%v", v, ok)
	}
}

func TestMessagePayloadTLVForLargeBody(t *testing.T) {
	// A body larger than 254 octets travels in the message_payload TLV with an empty short_message.
	large := bytes.Repeat([]byte("A"), 400)
	sm := &SubmitSM{SMFields{
		DestinationAddr: "22507000000",
		TLVs:            TLVList{{Tag: TagMessagePayload, Value: large}},
	}}
	b, err := Marshal(PDU{Sequence: 8, Body: sm})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	payload, ok := got.Body.(*SubmitSM).TLVs.Get(TagMessagePayload)
	if !ok || !bytes.Equal(payload, large) {
		t.Fatalf("message_payload not preserved (ok=%v len=%d)", ok, len(payload))
	}
}

func TestMarshalRejectsOverlongShortMessage(t *testing.T) {
	sm := &SubmitSM{SMFields{ShortMessage: bytes.Repeat([]byte("x"), 255)}}
	if _, err := Marshal(PDU{Body: sm}); err != ErrMessageTooLong {
		t.Fatalf("got %v, want ErrMessageTooLong", err)
	}
}

func TestUnmarshalRejectsMalformed(t *testing.T) {
	valid, _ := Marshal(PDU{Sequence: 1, Body: &EnquireLink{}})

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{"short header", []byte{0, 0, 0, 4}, ErrShortHeader},
		{"length too big", func() []byte { b := append([]byte{}, valid...); b[3] = 0xFF; return b }(), ErrLengthMismatch},
		{"length too small", func() []byte { b := append([]byte{}, valid...); b[3] = 0x0F; return b }(), ErrLengthMismatch},
		{"unknown command", func() []byte {
			b := append([]byte{}, valid...)
			b[4], b[5], b[6], b[7] = 0x00, 0x00, 0xAB, 0xCD
			return b
		}(), ErrUnknownCommand},
		{"trailing bytes on empty body", func() []byte {
			// enquire_link with a spurious extra octet: length says 17, body should be empty.
			b := make([]byte, 17)
			copy(b, valid)
			b[3] = 17
			return b
		}(), ErrMalformedBody},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Unmarshal(tc.data); err != tc.want {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestUnmarshalTruncatedSubmitSM(t *testing.T) {
	full, _ := Marshal(PDU{Sequence: 1, Body: &SubmitSM{SMFields{DestinationAddr: "123", ShortMessage: []byte("hi")}}})
	// Chop the body mid-field and fix command_length to the truncated size: must fail cleanly.
	for cut := headerLen + 1; cut < len(full); cut++ {
		truncated := append([]byte{}, full[:cut]...)
		truncated[0], truncated[1], truncated[2], truncated[3] = 0, 0, byte(cut>>8), byte(cut)
		if _, err := Unmarshal(truncated); err == nil {
			t.Fatalf("cut=%d: expected an error on truncated body", cut)
		}
	}
}

// commandName gives a readable subtest label for a command id.
func commandName(id CommandID) string {
	switch id {
	case CmdBindTransmitter:
		return "bind_transmitter"
	case CmdBindReceiver:
		return "bind_receiver"
	case CmdBindTransceiver:
		return "bind_transceiver"
	case CmdBindTransmitterResp:
		return "bind_transmitter_resp"
	case CmdBindReceiverResp:
		return "bind_receiver_resp"
	case CmdBindTransceiverResp:
		return "bind_transceiver_resp"
	case CmdSubmitSM:
		return "submit_sm"
	case CmdSubmitSMResp:
		return "submit_sm_resp"
	case CmdDeliverSM:
		return "deliver_sm"
	case CmdDeliverSMResp:
		return "deliver_sm_resp"
	case CmdEnquireLink:
		return "enquire_link"
	case CmdEnquireLinkResp:
		return "enquire_link_resp"
	case CmdUnbind:
		return "unbind"
	case CmdUnbindResp:
		return "unbind_resp"
	case CmdGenericNACK:
		return "generic_nack"
	default:
		return "unknown"
	}
}
