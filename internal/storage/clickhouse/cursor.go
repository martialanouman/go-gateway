package clickhouse

import "github.com/martialanouman/go-gateway/internal/platform/keyset"

// EncodeCDRCursor renders a keyset position as an opaque token, at MILLISECOND precision to match the
// CDR's DateTime64(3) column and the toUnixTimestamp64Milli comparison of the keyset queries.
func EncodeCDRCursor(k CDRKey) string {
	return keyset.Encode(keyset.Key{At: k.SubmittedAt, ID: k.MessageID}, keyset.Milli)
}

// DecodeCDRCursor parses a token produced by EncodeCDRCursor; a malformed one is an errs.ErrValidation.
func DecodeCDRCursor(s string) (CDRKey, error) {
	k, err := keyset.Decode(s, keyset.Milli)
	if err != nil {
		return CDRKey{}, err
	}
	return CDRKey{SubmittedAt: k.At, MessageID: k.ID}, nil
}
