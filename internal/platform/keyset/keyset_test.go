package keyset_test

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/keyset"
)

// The vectors were computed by hand (python: int(t.timestamp())*1e6 + microsecond, then base64url
// without padding) so the test is not the codec checking itself.
var (
	vectorID  = uuid.MustParse("018f6a2e-1c3b-7c4d-8e5f-0a1b2c3d4e5f")
	vectorAt  = time.Date(2026, 9, 3, 10, 0, 0, 123_456_000, time.UTC)
	vectorTok = map[keyset.Precision]string{
		keyset.Micro: "MTc4ODQyOTYwMDEyMzQ1NnwwMThmNmEyZS0xYzNiLTdjNGQtOGU1Zi0wYTFiMmMzZDRlNWY",
		keyset.Milli: "MTc4ODQyOTYwMDEyM3wwMThmNmEyZS0xYzNiLTdjNGQtOGU1Zi0wYTFiMmMzZDRlNWY",
	}
	vectorBack = map[keyset.Precision]time.Time{
		keyset.Micro: vectorAt,
		keyset.Milli: vectorAt.Truncate(time.Millisecond),
	}
)

func TestKnownVectorsEncodeAndDecode(t *testing.T) {
	t.Parallel()
	for p, tok := range vectorTok {
		if got := keyset.Encode(keyset.Key{At: vectorAt, ID: vectorID}, p); got != tok {
			t.Errorf("Encode(precision %d) = %q, want %q", p, got, tok)
		}
		k, err := keyset.Decode(tok, p)
		if err != nil {
			t.Fatalf("Decode(precision %d): %v", p, err)
		}
		if !k.At.Equal(vectorBack[p]) || k.ID != vectorID {
			t.Errorf("Decode(precision %d) = (%s, %s), want (%s, %s)", p, k.At, k.ID, vectorBack[p], vectorID)
		}
	}
}

// TestRoundTripKeepsMicrosAndTruncatesToMillis: the precision is the caller's column precision, and a
// cursor must land exactly on a storable instant — a finer one points between two rows and skips one.
func TestRoundTripKeepsMicrosAndTruncatesToMillis(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 1, 12, 0, 0, 123_456_789, time.UTC)
	id := uuid.New()
	for p, want := range map[keyset.Precision]time.Time{
		keyset.Micro: at.Truncate(time.Microsecond),
		keyset.Milli: at.Truncate(time.Millisecond),
	} {
		got, err := keyset.Decode(keyset.Encode(keyset.Key{At: at, ID: id}, p), p)
		if err != nil {
			t.Fatalf("precision %d: %v", p, err)
		}
		if !got.At.Equal(want) || got.ID != id {
			t.Errorf("precision %d: round trip = (%s, %s), want (%s, %s)", p, got.At, got.ID, want, id)
		}
		if got.At.Location() != time.UTC {
			t.Errorf("precision %d: decoded time is not UTC", p)
		}
	}
}

func TestDecodeRejectsMalformedTokensAsValidationErrors(t *testing.T) {
	t.Parallel()
	tokens := map[string]string{
		"not base64":    "!!!!",
		"padded base64": base64.URLEncoding.EncodeToString([]byte("1754049600000|" + uuid.NewString())),
		"no separator":  base64.RawURLEncoding.EncodeToString([]byte("1754049600000")),
		"bad timestamp": base64.RawURLEncoding.EncodeToString([]byte("x|" + uuid.NewString())),
		"bad id":        base64.RawURLEncoding.EncodeToString([]byte("1754049600000|not-a-uuid")),
		"empty":         "",
	}
	for name, tok := range tokens {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := keyset.Decode(tok, keyset.Milli)
			if err == nil {
				t.Fatalf("accepted a malformed cursor %q", tok)
			}
			if !errors.Is(err, errs.ErrValidation) {
				t.Errorf("error %v does not carry errs.ErrValidation: a caller's FromError would answer 500, not 422", err)
			}
		})
	}
}
