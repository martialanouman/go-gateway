package clickhouse

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// cdrCursorSep separates the two keyset fields inside the pre-base64 payload. A message_id is a UUID
// (no separator char) and the timestamp is decimal, so '|' is unambiguous.
const cdrCursorSep = "|"

// EncodeCDRCursor renders a keyset position as an opaque base64url token.
//
// The timestamp is encoded at MILLISECOND precision to match the CDR's DateTime64(3) column, so a
// decoded cursor round-trips to exactly a storable instant; a finer precision would point between two
// rows and silently skip one.
//
// It lives beside CDRKey — of which it is the serialisation — rather than in each caller: a second
// copy would drift, and the symptom of a drifting keyset (a page lost when several messages share a
// millisecond) only shows up under load, long after the change that caused it.
func EncodeCDRCursor(k CDRKey) string {
	payload := strconv.FormatInt(k.SubmittedAt.UnixMilli(), 10) + cdrCursorSep + k.MessageID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

// DecodeCDRCursor parses a token produced by EncodeCDRCursor. Any malformed input is an error, which
// callers map to a 422: a cursor is opaque, so a client should only ever echo one back verbatim.
func DecodeCDRCursor(s string) (CDRKey, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return CDRKey{}, err
	}
	ts, id, ok := strings.Cut(string(raw), cdrCursorSep)
	if !ok {
		return CDRKey{}, errors.New("cursor: missing separator")
	}
	ms, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return CDRKey{}, err
	}
	messageID, err := uuid.Parse(id)
	if err != nil {
		return CDRKey{}, err
	}
	return CDRKey{SubmittedAt: time.UnixMilli(ms).UTC(), MessageID: messageID}, nil
}
