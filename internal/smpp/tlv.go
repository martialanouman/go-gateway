package smpp

// Well-known TLV tags (SMPP v3.4 §5.3.2) the gateway reads or writes. Others are preserved
// verbatim on a round-trip but not interpreted.
const (
	// TagMessagePayload carries a message body too large for the 254-octet short_message field.
	// When present, short_message is empty and sm_length is 0 (specification §5.1).
	TagMessagePayload uint16 = 0x0424
	// TagReceiptedMessageID carries the SMSC message id a delivery receipt refers to (M4).
	TagReceiptedMessageID uint16 = 0x001E
	// TagMessageState carries the delivery state in a receipt (M4).
	TagMessageState uint16 = 0x0427
	// TagSCInterfaceVersion is returned by an SMSC in a bind response to advertise its version.
	TagSCInterfaceVersion uint16 = 0x0210
)

// TLV is a tag-length-value optional parameter. Length is implicit in len(Value) on encode.
type TLV struct {
	Tag   uint16
	Value []byte
}

// TLVList is the ordered set of optional parameters trailing a PDU body. Order is preserved on a
// round-trip.
type TLVList []TLV

// Get returns the value of the first TLV with the given tag, and whether it was present.
func (l TLVList) Get(tag uint16) ([]byte, bool) {
	for _, t := range l {
		if t.Tag == tag {
			return t.Value, true
		}
	}
	return nil, false
}

// Set appends a TLV. It does not deduplicate: SMPP permits repeated tags, and the pipeline never
// needs to replace one in place.
func (l *TLVList) Set(tag uint16, value []byte) {
	*l = append(*l, TLV{Tag: tag, Value: value})
}

func (l TLVList) marshal(w *writer) {
	for _, t := range l {
		if len(t.Value) > 0xFFFF {
			if w.err == nil {
				w.err = ErrTLVTooLong
			}
			return
		}
		w.u16(t.Tag)
		w.u16(uint16(len(t.Value))) //nolint:gosec // length guarded to <=0xFFFF just above
		w.octets(t.Value)
	}
}

// readTLVs consumes the remainder of the body as a sequence of TLVs. A trailing fragment shorter
// than a 4-octet TLV header, or a length that runs past the body, is a malformed frame.
func readTLVs(r *reader) TLVList {
	var out TLVList
	for r.err == nil && r.remaining() > 0 {
		tag := r.u16()
		length := int(r.u16())
		value := r.octetString(length)
		if r.err != nil {
			return out
		}
		out = append(out, TLV{Tag: tag, Value: value})
	}
	return out
}
