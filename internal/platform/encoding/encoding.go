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

// FromDataCoding maps an SMPP data_coding byte (SMPP v3.4 §5.2.19 / GSM 03.38) to the concrete
// pipeline encoding. It is the inverse of the connector's encoding->data_coding derivation and the
// value an SMPP submit resolves its Encoding to, since an ESME expresses its coding through
// data_coding rather than the REST encoding enum.
//
// The message-class range 0xF0-0xFF has no UCS-2 form: bit 2 selects 8-bit (binary), otherwise GSM-7.
// The general range keys on the character-set bits (3-2): 0b10 is UCS-2, 0b01 is 8-bit; everything
// else — including the default alphabet 0x00 and any reserved coding — is GSM-7.
func FromDataCoding(dataCoding uint8) string {
	if dataCoding&0xF0 == 0xF0 {
		if dataCoding&0x04 != 0 {
			return Binary
		}
		return GSM7
	}
	switch dataCoding & 0x0C {
	case 0x08:
		return UCS2
	case 0x04:
		return Binary
	default:
		return GSM7
	}
}
