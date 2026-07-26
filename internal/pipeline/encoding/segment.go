// Package encoding (pipeline) splits an MT into the concatenated segments the SMSC wire carries. It
// complements internal/platform/encoding, which detects the encoding and counts segments; this
// package produces the actual per-segment short_message octet strings (a concatenation UDH followed
// by the encoded content when there is more than one segment).
package encoding

import (
	"encoding/binary"
	"unicode/utf16"

	"github.com/google/uuid"

	platenc "github.com/martialanouman/go-gateway/internal/platform/encoding"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// Per-encoding SMS payload limits (GSM 03.40). A single-segment message uses the full payload; a
// concatenated one carries a 7-octet concatenation UDH (a 16-bit reference — see EncodeConcatUDH with
// Ref16), shrinking each segment. These mirror the counting limits in internal/platform/encoding so
// Split and DetectAndCount always agree.
const (
	gsm7Single = 160 // septets
	gsm7Multi  = 152 // septets (8 lost to the 7-octet UDH, rounded up to septets)
	ucs2Single = 70  // UTF-16 code units
	ucs2Multi  = 66  // UTF-16 code units (133 octets / 2, rounded down)
	binSingle  = 140 // octets
	binMulti   = 133 // octets
)

// Segment is one part of a (possibly concatenated) message, ready for the SMSC wire. Payload is the
// short_message octet string: a concatenation UDH followed by the encoded content when Total > 1, the
// bare encoded content when Total == 1. HasUDH says whether the caller must set esm_class's UDH
// indicator. Seq is 1-based; every segment of a message shares Ref and Total.
type Segment struct {
	Seq     int
	Total   int
	Ref     uint16
	Payload []byte
	HasUDH  bool
}

// Split divides body into the segments its encoding needs, DETERMINISTICALLY: the concatenation
// reference is derived from messageID, so any at-least-once replay of the split produces byte-for-byte
// identical segments under the same reference — a handset always reassembles them, and a per-segment
// dedup key (messageID, seq) is stable. A short message is a single segment with no UDH. Split is pure
// and does no I/O; body is read in memory only and never logged (invariant a).
func Split(messageID uuid.UUID, body []byte, enc string) []Segment {
	ref := concatRef(messageID)

	var parts [][]byte
	switch enc {
	case platenc.UCS2:
		parts = splitByCost(body, ucs2Single, ucs2Multi, ucs2Cost, encodeUCS2)
	case platenc.Binary:
		parts = splitBytes(body, binSingle, binMulti)
	default: // GSM-7
		parts = splitByCost(body, gsm7Single, gsm7Multi, gsm7Cost, encodeGSM7)
	}

	total := len(parts)
	segs := make([]Segment, total)
	for i, content := range parts {
		seq := i + 1
		if total == 1 {
			segs[i] = Segment{Seq: seq, Total: 1, Ref: ref, Payload: content}
			continue
		}
		//nolint:gosec // total/seq are SMPP 8-bit concat fields; a >255-segment SMS (>39k chars) is
		// pathological and the SMSC rejects it — the wire simply cannot express more.
		udh := smpp.EncodeConcatUDH(smpp.Concat{Reference: ref, Total: uint8(total), Sequence: uint8(seq), Ref16: true}, content)
		segs[i] = Segment{Seq: seq, Total: total, Ref: ref, Payload: udh, HasUDH: true}
	}
	return segs
}

// concatRef derives a 16-bit concatenation reference from the logical message id, DETERMINISTICALLY
// so a replay reuses the same reference. It reads the LAST two octets, never the first: MessageID is a
// UUIDv7 whose leading octets are the high bits of its millisecond timestamp — they only change once
// every ~50 days, so a reference taken from them would be shared by every long message in that window
// and a handset would merge the segments of two unrelated messages sent to one recipient. The trailing
// octets are the v7 rand_b field (full entropy), so distinct messages get distinct references and
// collisions stay rare at high volume.
func concatRef(messageID uuid.UUID) uint16 {
	return binary.BigEndian.Uint16(messageID[14:])
}

// splitByCost greedily packs runes into segments whose cumulative cost stays within the limit, never
// splitting a rune (so a GSM-7 escape sequence and a UCS-2 surrogate pair each stay whole). It uses
// the single-segment limit when everything fits in one segment, else the smaller multi-segment limit.
func splitByCost(body []byte, single, multi int, cost func(rune) int, encode func([]rune) []byte) [][]byte {
	runes := []rune(string(body))
	total := 0
	for _, r := range runes {
		total += cost(r)
	}
	if total <= single {
		return [][]byte{encode(runes)}
	}

	var parts [][]byte
	start, cur := 0, 0
	for i, r := range runes {
		c := cost(r)
		if cur+c > multi {
			parts = append(parts, encode(runes[start:i]))
			start, cur = i, 0
		}
		cur += c
	}
	return append(parts, encode(runes[start:]))
}

// splitBytes packs raw octets into segments (8-bit binary content).
func splitBytes(body []byte, single, multi int) [][]byte {
	if len(body) <= single {
		return [][]byte{body}
	}
	var parts [][]byte
	for i := 0; i < len(body); i += multi {
		end := i + multi
		if end > len(body) {
			end = len(body)
		}
		parts = append(parts, body[i:end])
	}
	return parts
}

// gsm7Cost is a rune's septet cost: two for an extension character (escape + char), one otherwise
// (a basic character, or a non-representable one the encoder substitutes with '?').
func gsm7Cost(r rune) int {
	if platenc.IsGSM7Extension(r) {
		return 2
	}
	return 1
}

// ucs2Cost is a rune's UTF-16 code-unit cost: two for a supplementary character (a surrogate pair),
// one otherwise.
func ucs2Cost(r rune) int {
	if r > 0xFFFF {
		return 2
	}
	return 1
}

// encodeGSM7 renders runes as the GSM-7 wire content. It mirrors the connector's existing GSM-7
// handling (the UTF-8 bytes, data_coding 0), so segmentation adds the UDH without changing the byte
// representation of the content.
func encodeGSM7(runes []rune) []byte {
	return []byte(string(runes))
}

// encodeUCS2 renders runes as big-endian UTF-16 (UCS-2 on the SMPP wire).
func encodeUCS2(runes []rune) []byte {
	units := utf16.Encode(runes)
	out := make([]byte, len(units)*2)
	for i, u := range units {
		binary.BigEndian.PutUint16(out[i*2:], u)
	}
	return out
}
