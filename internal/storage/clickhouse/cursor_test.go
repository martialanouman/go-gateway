package clickhouse_test

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

func TestCDRCursorRoundTripsAtMillisecondPrecision(t *testing.T) {
	t.Parallel()

	// The sub-millisecond component must NOT survive: the CDR column is DateTime64(3), so a cursor
	// carrying microseconds would point between two storable instants and skip a row.
	want := clickhouse.CDRKey{
		SubmittedAt: time.Date(2026, 8, 1, 12, 0, 0, 123_456_789, time.UTC),
		MessageID:   uuid.New(),
	}

	got, err := clickhouse.DecodeCDRCursor(clickhouse.EncodeCDRCursor(want))
	if err != nil {
		t.Fatalf("DecodeCDRCursor: %v", err)
	}
	if wantAt := want.SubmittedAt.Truncate(time.Millisecond); !got.SubmittedAt.Equal(wantAt) {
		t.Errorf("SubmittedAt = %s, want %s", got.SubmittedAt, wantAt)
	}
	if got.MessageID != want.MessageID {
		t.Errorf("MessageID = %s, want %s", got.MessageID, want.MessageID)
	}
}

func TestDecodeCDRCursorRejectsMalformedTokens(t *testing.T) {
	t.Parallel()

	// The payloads are encoded here rather than pasted as literals: a hand-written token with base64
	// padding would be rejected by the decoder for the wrong reason and prove nothing.
	tokens := map[string]string{
		"not base64":     "!!!!",
		"no separator":   base64.RawURLEncoding.EncodeToString([]byte("1754049600000")),
		"bad timestamp":  base64.RawURLEncoding.EncodeToString([]byte("x|" + uuid.NewString())),
		"bad message id": base64.RawURLEncoding.EncodeToString([]byte("1754049600000|not-a-uuid")),
	}

	for name, token := range tokens {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if _, err := clickhouse.DecodeCDRCursor(token); err == nil {
				t.Errorf("accepted a malformed cursor (%q)", token)
			}
		})
	}
}
