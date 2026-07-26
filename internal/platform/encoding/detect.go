package encoding

// Per-encoding SMS payload limits (GSM 03.40). A single-segment message uses the full payload; a
// concatenated one reserves 6 octets for the concatenation UDH, shrinking each segment.
const (
	gsm7Single = 160 // septets
	gsm7Multi  = 153 // septets (7 lost to the UDH, rounded to septets)
	ucs2Single = 70  // UTF-16 code units (140 octets / 2)
	ucs2Multi  = 67  // UTF-16 code units (134 octets / 2)
	binSingle  = 140 // octets
	binMulti   = 134 // octets
)

// DetectAndCount resolves the encoding for a message and counts the segments it needs, WITHOUT
// splitting it (step-082 does the actual UDH segmentation). It is pure and does no I/O; body is the
// revealed message body, used in memory only and never logged (invariant a).
//
// Precedence (§6.6): an explicit client encoding (gsm7|ucs2|binary) wins; otherwise (auto) the
// connector's data_coding_default is honoured when set; otherwise the content is auto-detected —
// GSM-7 if every rune is representable, else UCS-2. segment_count is always >= 1.
func DetectAndCount(requested string, dataCodingDefault *int, body []byte) (enc string, segments int) {
	enc = resolveEncoding(requested, dataCodingDefault, body)
	return enc, segmentCount(enc, body)
}

func resolveEncoding(requested string, dataCodingDefault *int, body []byte) string {
	switch requested {
	case GSM7, UCS2, Binary:
		return requested // an explicit client override wins
	default: // Auto (or anything unrecognised)
		if dataCodingDefault != nil {
			//nolint:gosec // a data_coding is a single octet; the column is a smallint carrying 0-255.
			return FromDataCoding(uint8(*dataCodingDefault))
		}
		return autoDetect(body)
	}
}

// autoDetect picks GSM-7 when the body is fully representable in the 7-bit alphabet, else UCS-2. It
// never returns Binary: binary content cannot be inferred from text and is only reached via an
// explicit request or a connector data_coding.
func autoDetect(body []byte) string {
	if gsm7Representable(body) {
		return GSM7
	}
	return UCS2
}

// segmentCount returns how many segments the body needs in the given encoding (>= 1). GSM-7 is always
// counted in septets — a client that forced GSM-7 on non-representable content gets each such rune
// substituted by a single '?', so the label and the count stay consistent (the message is sent as
// GSM-7, not silently promoted to UCS-2).
func segmentCount(enc string, body []byte) int {
	switch enc {
	case UCS2:
		return count(ucs2Units(body), ucs2Single, ucs2Multi)
	case Binary:
		return count(len(body), binSingle, binMulti)
	default: // GSM7
		return count(gsm7Septets(body), gsm7Single, gsm7Multi)
	}
}

// count returns the number of segments a payload of the given length needs, given the single- and
// multi-segment limits. An empty message is still one segment.
func count(length, single, multi int) int {
	if length <= single {
		return 1
	}
	return (length + multi - 1) / multi
}

// ucs2Units counts the UTF-16 code units the body occupies: a BMP rune is one unit, a supplementary
// rune (emoji, rare CJK, > U+FFFF) is a surrogate pair of two — which is exactly how UCS-2 SMS bills
// it. Ranging over the string never yields a lone surrogate, so this matches utf16.Encode without its
// per-rune allocation (the hot path runs at thousands of messages/second).
func ucs2Units(body []byte) int {
	units := 0
	for _, r := range string(body) {
		if r > 0xFFFF {
			units += 2
		} else {
			units++
		}
	}
	return units
}
