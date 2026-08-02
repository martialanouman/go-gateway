package connectorpool

// The pure mapping layer between a routed message and the wire: what a submit_sm carries, and what
// the CDR records of it. Every function here is stateless and total — no Service, no I/O, no error —
// which is why they sit apart from the orchestration in connectorpool.go: they are the part of the
// connector that can be reasoned about (and tested) one value at a time.

import (
	"strings"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/encoding"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// buildSubmit maps one routed SEGMENT onto a submit_sm. Body already carries the segment's wire
// short_message — the concatenation UDH followed by the encoded content when the message spans several
// segments, the bare encoded content when it does not (internal/pipeline/encoding.Split produced it in
// the resolved encoding), so the connector no longer encodes: it puts the bytes on the wire verbatim.
// Revealing the body here is an audited egress (like the Kafka payload): the plaintext goes onto the
// SMSC wire, never into a log or span. When the segment begins with a UDH, esm_class's UDH indicator is
// set so the SMSC and the handset parse and reassemble it.
func buildSubmit(r pipeline.RoutedMT) *smpp.SubmitSM {
	source, sourceTON, sourceNPI := sourceAddr(r.From)
	sm := &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddr:      source,
		DestinationAddr: r.To,
		DataCoding:      submitDataCoding(r),
	}}
	sm.SourceAddrTON, sm.SourceAddrNPI = sourceTON, sourceNPI
	sm.DestAddrTON, sm.DestAddrNPI = smpp.TONInternational, smpp.NPIISDN
	if r.RegisteredDelivery {
		sm.RegisteredDelivery = smpp.RegisteredDeliveryReceipt
	}
	if r.HasUDH {
		sm.ESMClass = smpp.ESMClassUDHIndicator
	}
	// The SMPP validity_period is a 16-char C-Octet String; a longer value would marshal a PDU with no
	// NUL terminator, which the SMSC rejects by dropping the connection — poisoning the partition on
	// redelivery. REST bounds it (maxLength 16), but guard here too so a malformed mt.routed record can
	// never crash the bind: an over-length value is dropped rather than sent.
	if r.ValidityPeriod != nil && len(*r.ValidityPeriod) <= 16 {
		sm.ValidityPeriod = *r.ValidityPeriod
	}

	body := r.Body.Reveal() // audited: segment wire bytes -> SMSC wire, never logged
	if len(body) > 254 {
		// A segment normally fits in short_message: UCS-2 (<=133 octets) and binary (<=133) always do,
		// and GSM-7 does once bit-packed. Until packing lands, an accented GSM-7 segment carried as
		// unpacked UTF-8 can exceed 254 octets; fall back to message_payload so an over-length PDU never
		// poisons the bind. Concatenation is degraded in that path — the fix is GSM-7 packing (follow-up).
		sm.TLVs.Set(smpp.TagMessagePayload, body)
	} else {
		sm.ShortMessage = body
	}
	return sm
}

// submitDataCoding is the wire data_coding byte. An explicit, in-range client override wins (the
// client is driving the DCS directly); otherwise it is derived from the resolved encoding.
func submitDataCoding(r pipeline.RoutedMT) uint8 {
	if dc := r.DataCoding; dc != nil && *dc >= 0 && *dc <= 255 {
		return uint8(*dc) //nolint:gosec // bounded to 0..255 on the line above
	}
	return dataCoding(r.Encoding)
}

// submitOutcome builds the enroute (or failed) outcome event from the submit_sm_resp — the projection
// the connector used to write to ClickHouse itself, now published on mt.outcome for a dedicated
// consumer to turn into the CDR row (step-201c, D1).
//
// The segment coordinates are CLAMPED here, not left to the projection: segment_seq joins the CDR
// sorting key and its 0 is reserved for the pre-dispatch message-level row, so a connector outcome that
// reached the projection unclamped would land on the wrong row instead of superseding its own. Clamping
// at the only place that knows a connector row is always a dispatched segment leaves the consumer a
// straight field copy. Encoding travels as the resolved pipeline string; the projection maps it with
// clickhouse.EncodingOf, the total projection every CDR producer already shares. No body: the outcome
// row stores no content.
func submitOutcome(r pipeline.RoutedMT, resp smpp.PDU) pipeline.OutcomeMT {
	status, errorCode := outcome(resp.Status)
	return pipeline.OutcomeMT{
		MessageID:    r.MessageID,
		TraceID:      r.TraceID,
		AccountID:    r.AccountID,
		CustomerID:   r.CustomerID,
		ConnectorID:  r.ConnectorID,
		RouteID:      r.RouteID,
		From:         r.From,
		To:           r.To,
		Encoding:     r.Encoding,
		SegmentSeq:   int(segmentSeq(r.SegmentSeq)),
		SegmentCount: int(segmentCount(r.SegmentCount)),
		SubmittedAt:  r.SubmittedAt,
		Status:       string(status),
		ErrorCode:    errorCode,
	}
}

// cancelledRow builds the cancelled CDR row (rank 60) for a message a cancel_sm flagged before
// dispatch. It mirrors cdrRow's identifier projection but records no connector outcome (the message
// was never submitted). It is written in addition to the Canceller's own row: idempotent under
// ReplacingMergeTree (same ORDER BY key and rank), it closes the crash window between flag and row.
func cancelledRow(r pipeline.RoutedMT) clickhouse.CDRRow {
	return clickhouse.CDRRow{
		MessageID:    r.MessageID,
		TraceID:      r.TraceID,
		AccountID:    r.AccountID,
		CustomerID:   r.CustomerID,
		Direction:    clickhouse.DirectionMT,
		SourceAddr:   r.From,
		DestAddr:     r.To,
		SubmittedAt:  r.SubmittedAt,
		Status:       clickhouse.StatusCancelled,
		SegmentCount: segmentCount(r.SegmentCount),
		SegmentSeq:   segmentSeq(r.SegmentSeq),
		Encoding:     clickhouse.EncodingOf(r.Encoding),
		Billed:       false,
	}
}

// outcome maps a submit_sm_resp command_status to a CDR status. ESME_ROK is enroute; anything else
// is a failed send, with an error_code drawn from the shared platform/errors contract (never an
// ad-hoc string), so a client reading GET /messages sees a documented code.
func outcome(cmdStatus uint32) (clickhouse.Status, *string) {
	if cmdStatus == smpp.StatusOK {
		return clickhouse.StatusEnroute, nil
	}
	code := string(errs.CodeFromSMPPStatus(cmdStatus))
	return clickhouse.StatusFailed, &code
}

// dataCoding derives the SMPP data_coding byte from the resolved encoding, using the shared encoding
// vocabulary (internal/platform/encoding) so the value set does not drift across the pipeline.
func dataCoding(enc string) uint8 {
	switch enc {
	case encoding.UCS2:
		return smpp.DataCodingUCS2
	case encoding.Binary:
		return smpp.DataCodingBinary
	default:
		return smpp.DataCodingGSM7
	}
}

// sourceAddr maps a submitted source address to its wire form and TON/NPI. A numeric MSISDN
// (optionally "+"-prefixed) becomes a plus-stripped international/ISDN address; anything else passes
// through as an alphanumeric sender id. Source normalization proper is an M5 concern — this is only
// the wire typing the SMSC requires, so a "+1206…" MSISDN is not mistyped as an alphanumeric sender.
func sourceAddr(addr string) (wire string, ton, npi uint8) {
	digits := strings.TrimPrefix(addr, "+")
	if digits != "" && isAllDigits(digits) {
		return digits, smpp.TONInternational, smpp.NPIISDN
	}
	return addr, smpp.TONAlphanumeric, smpp.NPIUnknown
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func segmentCount(n int) uint16 {
	if n < 1 {
		return 1
	}
	return uint16(n) //nolint:gosec // segment count is a small positive integer
}

// segmentSeq maps a routed segment's 1-based sequence to the CDR column. A connector row is always a
// dispatched segment, so a missing/zero value defaults to 1 (never 0, which the read path reserves for
// the pre-dispatch message-level row).
func segmentSeq(n int) uint16 {
	if n < 1 {
		return 1
	}
	return uint16(n) //nolint:gosec // segment sequence is a small positive integer
}
