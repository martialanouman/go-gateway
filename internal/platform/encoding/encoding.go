// Package encoding holds the message-encoding vocabulary shared across the MT pipeline: the four
// values a client may request and the rule that resolves a request to a concrete encoding. Every
// layer that switches on an encoding string — the pipeline's resolution stage, the CDR projection,
// the SMSC data_coding mapping — draws its constants from here, so a new encoding is added in one
// place instead of drifting across three switches.
//
// The projections themselves stay in their layers because their result types differ (a resolved
// string, a clickhouse.Encoding, an SMPP data_coding byte); only the vocabulary and the auto->gsm7
// resolution rule are centralised.
package encoding

// The encoding values. Auto is a request-only value the client may send; Resolve turns it (and any
// unknown value) into a concrete encoding. GSM7/UCS2/Binary are the resolved encodings a CDR row and
// the SMSC leg key on.
const (
	Auto   = "auto"
	GSM7   = "gsm7"
	UCS2   = "ucs2"
	Binary = "binary"
)

// Resolve maps a requested encoding to the concrete encoding the rest of the pipeline uses. M2 does
// not auto-detect: Auto — and anything unrecognised — resolves to GSM-7.
func Resolve(requested string) string {
	switch requested {
	case UCS2:
		return UCS2
	case Binary:
		return Binary
	default:
		return GSM7
	}
}
