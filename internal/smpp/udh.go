package smpp

import "errors"

// User Data Header support (GSM 03.40 §9.2.3.24). A concatenated (multipart) SMS carries a UDH at
// the front of short_message, flagged by the ESMClassUDHIndicator bit. M2 sends single segments,
// but the codec must build and parse the header so segmentation (a later milestone) and any
// inbound multipart message round-trip losslessly.
//
// UDH layout: a length octet (UDHL, the count of the octets that follow) then one or more
// information elements, each an identifier octet (IEI), a length octet (IEDL) and IEDL octets of
// data. The message content follows the header.

const (
	// ieiConcat8 is the concatenation IE with an 8-bit reference number (IEDL 3: ref, total, seq).
	ieiConcat8 uint8 = 0x00
	// ieiConcat16 is the concatenation IE with a 16-bit reference number (IEDL 4: ref hi/lo, total,
	// seq). A 16-bit reference reduces reassembly collisions on high-volume links.
	ieiConcat16 uint8 = 0x08
)

// ErrInvalidUDH is returned when a User Data Header is truncated or internally inconsistent.
var ErrInvalidUDH = errors.New("smpp: invalid user data header")

// Concat describes a segment's place in a concatenated message.
type Concat struct {
	// Reference groups the segments of one logical message; all its segments share it.
	Reference uint16
	// Total is the segment count of the logical message.
	Total uint8
	// Sequence is this segment's 1-based position.
	Sequence uint8
	// Ref16 selects a 16-bit reference number on encode; ParseUDH sets it from what it read.
	Ref16 bool
}

// EncodeConcatUDH builds a short_message octet string for one segment: a concatenation UDH
// followed by content. The caller must set ESMClassUDHIndicator on the PDU's esm_class.
func EncodeConcatUDH(c Concat, content []byte) []byte {
	var udh []byte
	if c.Ref16 {
		//nolint:gosec // deliberate octet extraction of a 16-bit reference number
		udh = []byte{ieiConcat16, 4, uint8(c.Reference >> 8), uint8(c.Reference), c.Total, c.Sequence}
	} else {
		udh = []byte{ieiConcat8, 3, uint8(c.Reference), c.Total, c.Sequence} //nolint:gosec // low octet of the reference
	}
	out := make([]byte, 0, 1+len(udh)+len(content))
	out = append(out, uint8(len(udh))) //nolint:gosec // UDH is at most 6 octets here
	out = append(out, udh...)
	out = append(out, content...)
	return out
}

// ParseUDH splits a short_message that begins with a User Data Header into its concatenation info
// (if a concatenation IE is present) and the trailing content. It never panics on malformed input;
// a truncated header returns ErrInvalidUDH. hasConcat is false when the header is well-formed but
// carries no concatenation element.
func ParseUDH(shortMessage []byte) (c Concat, content []byte, hasConcat bool, err error) {
	if len(shortMessage) == 0 {
		return Concat{}, nil, false, ErrInvalidUDH
	}
	udhl := int(shortMessage[0])
	if 1+udhl > len(shortMessage) {
		return Concat{}, nil, false, ErrInvalidUDH
	}
	header := shortMessage[1 : 1+udhl]
	content = shortMessage[1+udhl:]

	for i := 0; i < len(header); {
		if i+2 > len(header) {
			return Concat{}, nil, false, ErrInvalidUDH
		}
		iei := header[i]
		iedl := int(header[i+1]) //nolint:gosec // i+2 <= len(header) checked just above
		if i+2+iedl > len(header) {
			return Concat{}, nil, false, ErrInvalidUDH
		}
		ied := header[i+2 : i+2+iedl]
		switch {
		case iei == ieiConcat8 && iedl == 3:
			c = Concat{Reference: uint16(ied[0]), Total: ied[1], Sequence: ied[2]}
			hasConcat = true
		case iei == ieiConcat16 && iedl == 4:
			c = Concat{Reference: uint16(ied[0])<<8 | uint16(ied[1]), Total: ied[2], Sequence: ied[3], Ref16: true}
			hasConcat = true
		}
		i += 2 + iedl
	}
	return c, content, hasConcat, nil
}
