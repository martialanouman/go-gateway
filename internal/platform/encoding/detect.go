package encoding

// Per-encoding SMS payload limits (GSM 03.40). A single-segment message uses the full payload; a
// concatenated one carries a 7-octet concatenation UDH (a 16-bit reference — 1 UDHL + 1 IEI + 1 IEDL +
// 2 ref + 1 total + 1 seq), shrinking each segment. These mirror internal/pipeline/encoding so
// DetectAndCount and Split always report the same segment count.
const (
	gsm7Single = 160 // septets
	gsm7Multi  = 152 // septets (8 lost to the 7-octet UDH, rounded up to septets)
	ucs2Single = 70  // UTF-16 code units (140 octets / 2)
	ucs2Multi  = 66  // UTF-16 code units (133 octets / 2, rounded down)
	binSingle  = 140 // octets
	binMulti   = 133 // octets
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

// segmentCount returns how many segments the body needs in the given encoding (>= 1). GSM-7 and UCS-2
// are counted by the SAME greedy packing internal/pipeline/encoding uses to split, because a cost-2
// atom (a GSM-7 extension char, a UCS-2 surrogate pair) can never straddle a segment boundary: a naive
// ceil(total/limit) under-counts when such atoms leave a unit unfillable at the tail of a segment. A
// client that forced GSM-7 on non-representable content gets each such rune substituted by a single
// '?' (cost 1), so the label and the count stay consistent (sent as GSM-7, not promoted to UCS-2).
func segmentCount(enc string, body []byte) int {
	switch enc {
	case UCS2:
		return countByCost(body, ucs2Single, ucs2Multi, ucs2Cost)
	case Binary:
		return count(len(body), binSingle, binMulti)
	default: // GSM7
		return countByCost(body, gsm7Single, gsm7Multi, gsm7Cost)
	}
}

// count returns the number of segments a payload of the given length needs, given the single- and
// multi-segment limits. An empty message is still one segment. It suits fixed-width content (binary
// octets), where every unit costs one and greedy packing degenerates to a ceiling division.
func count(length, single, multi int) int {
	if length <= single {
		return 1
	}
	return (length + multi - 1) / multi
}

// countByCost counts the segments a rune sequence needs when each rune has a per-encoding cost and no
// rune may straddle a segment boundary. It greedily packs runes exactly as internal/pipeline/encoding
// splits them, so the count and the split never disagree. When everything fits in one segment the full
// single-segment limit applies; otherwise every segment carries the concatenation UDH and uses the
// smaller multi-segment limit.
func countByCost(body []byte, single, multi int, cost func(rune) int) int {
	total := 0
	for _, r := range string(body) {
		total += cost(r)
	}
	if total <= single {
		return 1
	}
	segments, cur := 1, 0
	for _, r := range string(body) {
		c := cost(r)
		if cur+c > multi {
			segments++
			cur = 0
		}
		cur += c
	}
	return segments
}

// gsm7Cost is a rune's septet cost: two for an extension character (escape 0x1B + char), one otherwise
// (a basic character, or a non-representable one the encoder substitutes with '?').
func gsm7Cost(r rune) int {
	if inSet(gsm7ExtensionSet, r) {
		return 2
	}
	return 1
}

// ucs2Cost is a rune's UTF-16 code-unit cost: two for a supplementary character (emoji, rare CJK,
// > U+FFFF — a surrogate pair), one otherwise. Ranging over the string never yields a lone surrogate.
func ucs2Cost(r rune) int {
	if r > 0xFFFF {
		return 2
	}
	return 1
}
