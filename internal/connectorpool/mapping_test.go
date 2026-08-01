package connectorpool_test

// The submit_sm mapping: what a routed segment actually puts on the wire. These tests moved out of
// connectorpool_test.go with mapping.go, unchanged — they exercise the same path through the
// Service, since the mapping helpers are unexported.

import (
	"bytes"
	"testing"

	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
)

func TestConnectorSubmitsBodyOnTheWire(t *testing.T) {
	const text = "the actual message body"
	var seen string
	r := routed()
	r.Body = msg.NewBodyString(text)
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = string(sm.ShortMessage)
		return fakesmsc.OK()
	}, r)

	if seen != text {
		t.Errorf("SMSC received short_message %q, want %q", seen, text)
	}
}

// TestConnectorShipsBodyVerbatim pins that the connector no longer encodes: Body already carries the
// segment's wire bytes (the pipeline's Split produced them in the resolved encoding), so a UCS-2
// payload — supplied here as the UTF-16BE bytes it is on the wire — reaches the SMSC unchanged, under
// the UCS-2 data_coding derived from the encoding. Transcoding now lives with segmentation, not here.
func TestConnectorShipsBodyVerbatim(t *testing.T) {
	wire := utf16BE("café ☕") // the UCS-2 wire bytes the pipeline would produce
	var seen []byte
	var dcs uint8
	r := routed()
	r.Encoding = "ucs2"
	r.Body = msg.NewBody(wire)
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = append([]byte(nil), sm.ShortMessage...)
		dcs = sm.DataCoding
		return fakesmsc.OK()
	}, r)

	if !bytes.Equal(seen, wire) {
		t.Errorf("body on the wire = % x, want it shipped verbatim % x", seen, wire)
	}
	if dcs != smpp.DataCodingUCS2 {
		t.Errorf("data_coding = %#x, want UCS-2 %#x", dcs, smpp.DataCodingUCS2)
	}
}

// TestConnectorHonorsDataCodingOverride pins that a client-supplied data_coding reaches the SMSC
// verbatim rather than being derived from the encoding.
func TestConnectorHonorsDataCodingOverride(t *testing.T) {
	override := 245 // a message-class / flash DCS the encoding would never produce
	var seen uint8
	r := routed()
	r.Encoding = "gsm7"
	r.DataCoding = &override
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = sm.DataCoding
		return fakesmsc.OK()
	}, r)

	if seen != uint8(override) {
		t.Errorf("data_coding = %d, want the client override %d", seen, override)
	}
}

// TestConnectorTypesNumericSourceAsInternational pins that a "+"-prefixed numeric MSISDN source is
// sent plus-stripped with international/ISDN typing, not as an alphanumeric sender id.
func TestConnectorTypesNumericSourceAsInternational(t *testing.T) {
	var addr string
	var ton, npi uint8
	r := routed()
	r.From = "+12065550100"
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		addr, ton, npi = sm.SourceAddr, sm.SourceAddrTON, sm.SourceAddrNPI
		return fakesmsc.OK()
	}, r)

	if addr != "12065550100" {
		t.Errorf("source addr = %q, want the plus-stripped MSISDN 12065550100", addr)
	}
	if ton != smpp.TONInternational || npi != smpp.NPIISDN {
		t.Errorf("source TON/NPI = %#x/%#x, want international/ISDN %#x/%#x", ton, npi, smpp.TONInternational, smpp.NPIISDN)
	}
}

// TestConnectorTypesAlphanumericSource keeps a non-numeric sender id typed as alphanumeric.
func TestConnectorTypesAlphanumericSource(t *testing.T) {
	var ton uint8
	r := routed()
	r.From = "ACME"
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		ton = sm.SourceAddrTON
		return fakesmsc.OK()
	}, r)

	if ton != smpp.TONAlphanumeric {
		t.Errorf("alphanumeric sender TON = %#x, want %#x", ton, smpp.TONAlphanumeric)
	}
}

// TestConnectorSetsUDHIndicatorForSegment pins that a segment carrying a concatenation UDH ships with
// esm_class's UDH indicator set and the payload (UDH + content) verbatim in short_message, so the SMSC
// and the handset parse and reassemble it. A single segment with no UDH leaves esm_class clear.
func TestConnectorSetsUDHIndicatorForSegment(t *testing.T) {
	payload := append([]byte{0x05, 0x00, 0x03, 0x2a, 0x02, 0x01}, []byte("part one")...) // 6-octet concat UDH + content
	var esm uint8
	var seen []byte
	r := routed()
	r.HasUDH = true
	r.SegmentSeq, r.SegmentCount = 1, 2
	r.Body = msg.NewBody(payload)
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		esm = sm.ESMClass
		seen = append([]byte(nil), sm.ShortMessage...)
		return fakesmsc.OK()
	}, r)

	if esm&smpp.ESMClassUDHIndicator == 0 {
		t.Errorf("esm_class = %#x, want the UDH indicator %#x set", esm, smpp.ESMClassUDHIndicator)
	}
	if !bytes.Equal(seen, payload) {
		t.Errorf("short_message = % x, want the UDH payload verbatim % x", seen, payload)
	}
}

// TestConnectorNoUDHIndicatorForSingleSegment pins the complement: a lone segment carries no UDH bit.
func TestConnectorNoUDHIndicatorForSingleSegment(t *testing.T) {
	var esm uint8
	r := routed() // HasUDH false
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		esm = sm.ESMClass
		return fakesmsc.OK()
	}, r)

	if esm&smpp.ESMClassUDHIndicator != 0 {
		t.Errorf("esm_class = %#x, want the UDH indicator clear for a single segment", esm)
	}
}

// TestConnectorOverlongSegmentFallsBackToMessagePayload pins the defensive guard: a segment whose
// encoded bytes exceed short_message's 254-octet limit (reachable for long accented GSM-7 until
// bit-packing lands) is carried in the message_payload TLV instead, so an over-length PDU never
// poisons the bind. The submit still completes — the SMSC accepts it — and short_message stays empty.
func TestConnectorOverlongSegmentFallsBackToMessagePayload(t *testing.T) {
	payload := bytes.Repeat([]byte{0x41}, 300) // > 254 octets
	var short []byte
	var tlv []byte
	var haveTLV bool
	r := routed()
	r.HasUDH = true
	r.Body = msg.NewBody(payload)
	_, err := runService(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		short = append([]byte(nil), sm.ShortMessage...)
		tlv, haveTLV = sm.TLVs.Get(smpp.TagMessagePayload)
		return fakesmsc.OK()
	}, r)
	if err != nil {
		t.Fatalf("an over-length segment must still submit, not crash the bind: %v", err)
	}
	if len(short) != 0 {
		t.Errorf("short_message = % x, want empty (payload moved to the TLV)", short)
	}
	if !haveTLV || !bytes.Equal(tlv, payload) {
		t.Errorf("message_payload TLV present=%v, want the payload verbatim", haveTLV)
	}
}

// TestConnectorSetsValidityPeriod pins that the client's validity_period reaches the submit_sm.
func TestConnectorSetsValidityPeriod(t *testing.T) {
	vp := "000000010000000R" // SMPP relative validity (16 chars), passed through per the contract
	var seen string
	r := routed()
	r.ValidityPeriod = &vp
	_ = runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = sm.ValidityPeriod
		return fakesmsc.OK()
	}, r)

	if seen != vp {
		t.Errorf("validity_period on the wire = %q, want %q", seen, vp)
	}
}

// TestConnectorDropsOverlongValidityPeriod pins the poison-loop guard: a validity_period longer than
// the 16-char SMPP C-Octet String is dropped rather than marshalled into an unterminated PDU that the
// SMSC would reject by dropping the bind (blocking the partition on redelivery). The submit still
// succeeds, just without a validity.
func TestConnectorDropsOverlongValidityPeriod(t *testing.T) {
	overlong := "00000001000000000000R" // 21 chars, > 16
	var seen string
	r := routed()
	r.ValidityPeriod = &overlong
	cdr := runOnce(t, func(sm smpp.SubmitSM) fakesmsc.Resp {
		seen = sm.ValidityPeriod
		return fakesmsc.OK()
	}, r)

	if seen != "" {
		t.Errorf("over-length validity_period should be dropped, wire carried %q", seen)
	}
	if len(cdr.rows) != 1 || cdr.rows[0].Status != clickhouse.StatusEnroute {
		t.Errorf("submit should still succeed (enroute), got %+v", cdr.rows)
	}
}
