package adminapi

import (
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// TestUnroutedCursorRoundTripPreservesMicroseconds guards the keyset precision: unrouted_mo.received_at
// is a microsecond timestamptz, so the cursor must round-trip microseconds exactly — a millisecond
// cursor would drop rows sharing a millisecond from the next page.
func TestUnroutedCursorRoundTripPreservesMicroseconds(t *testing.T) {
	// A time that is NOT millisecond-aligned (…123789 microseconds).
	at := time.UnixMicro(1_700_000_000_123_789).UTC()
	id := uuid.New()

	got, err := decodeUnroutedCursor(encodeUnroutedCursor(cp.UnroutedMOKey{ReceivedAt: at, ID: id}))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.ReceivedAt.Equal(at) {
		t.Errorf("received_at round trip = %v, want %v (sub-millisecond precision lost)", got.ReceivedAt, at)
	}
	if got.ID != id {
		t.Errorf("id round trip = %s, want %s", got.ID, id)
	}
}
