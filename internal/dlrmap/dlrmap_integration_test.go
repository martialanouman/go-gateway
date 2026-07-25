package dlrmap_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// routedFixture builds a routed envelope for the given connector/smsc mapping test.
func routedFixture(connectorID uuid.UUID, vp *string) pipeline.RoutedMT {
	return pipeline.RoutedMT{
		MessageID:      uuid.New(),
		TraceID:        uuid.New(),
		AccountID:      uuid.New(),
		CustomerID:     uuid.New(),
		From:           "GATEWAY",
		To:             "+22507000000",
		Body:           msg.NewBodyString("body should never be stored"),
		Encoding:       "gsm7",
		ConnectorID:    connectorID,
		SegmentCount:   1,
		ValidityPeriod: vp,
		SubmittedAt:    time.Now().UTC().Truncate(time.Millisecond),
	}
}

// TestRedisMapPutGetRoundTrip: a Put stores the full CDR projection under the composite key with a TTL
// derived from the validity_period, and Get reads it back — without the body.
func TestRedisMapPutGetRoundTrip(t *testing.T) {
	rdb := redistest.Client(t)
	store := dlrmap.NewRedisMap(rdb)
	ctx := context.Background()

	connectorID := uuid.New()
	smscID := "00000000000000ab"
	vp := "000001000000000R" // 1 day, relative
	r := routedFixture(connectorID, &vp)

	if err := store.Put(ctx, smscID, r); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, found, err := store.Get(ctx, connectorID, smscID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("Get found=false, want the mapping just written")
	}
	if got.MessageID != r.MessageID || got.TraceID != r.TraceID || got.AccountID != r.AccountID ||
		got.CustomerID != r.CustomerID || got.ConnectorID != r.ConnectorID ||
		got.SourceAddr != r.From || got.DestAddr != r.To || got.SegmentCount != r.SegmentCount ||
		got.Encoding != r.Encoding || !got.SubmittedAt.Equal(r.SubmittedAt) {
		t.Errorf("mapping = %+v, want projection of %+v", got, r)
	}

	// The stored value never contains the body plaintext.
	raw, _ := rdb.Get(ctx, "dlrmap:{"+connectorID.String()+":"+smscID+"}").Result()
	if len(raw) == 0 || strings.Contains(raw, "body should never be stored") {
		t.Errorf("stored value leaked the body (invariant a): %q", raw)
	}

	// TTL derived from the 1-day validity + margin (~25h).
	ttl, err := rdb.TTL(ctx, "dlrmap:{"+connectorID.String()+":"+smscID+"}").Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if ttl < 24*time.Hour || ttl > 26*time.Hour {
		t.Errorf("TTL = %v, want ~25h (validity 1d + margin)", ttl)
	}
}

// TestRedisMapGetMissing: an unknown (connector, smsc id) is a clean miss — found=false, nil error.
func TestRedisMapGetMissing(t *testing.T) {
	rdb := redistest.Client(t)
	store := dlrmap.NewRedisMap(rdb)

	_, found, err := store.Get(context.Background(), uuid.New(), "does-not-exist")
	if err != nil {
		t.Fatalf("Get(missing) error = %v, want nil", err)
	}
	if found {
		t.Error("Get(missing) found=true, want false")
	}
}

// TestRedisMapPutScopesByConnector: the same smsc_msg_id from two different connectors yields two
// distinct mappings, because connector_id is part of the key.
func TestRedisMapPutScopesByConnector(t *testing.T) {
	rdb := redistest.Client(t)
	store := dlrmap.NewRedisMap(rdb)
	ctx := context.Background()

	smscID := "deadbeefdeadbeef"
	connA, connB := uuid.New(), uuid.New()
	rA := routedFixture(connA, nil)
	rB := routedFixture(connB, nil)

	if err := store.Put(ctx, smscID, rA); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := store.Put(ctx, smscID, rB); err != nil {
		t.Fatalf("Put B: %v", err)
	}

	gotA, foundA, err := store.Get(ctx, connA, smscID)
	if err != nil || !foundA {
		t.Fatalf("Get A: found=%v err=%v", foundA, err)
	}
	gotB, foundB, err := store.Get(ctx, connB, smscID)
	if err != nil || !foundB {
		t.Fatalf("Get B: found=%v err=%v", foundB, err)
	}
	if gotA.MessageID != rA.MessageID || gotB.MessageID != rB.MessageID {
		t.Errorf("scoping wrong: A=%s (want %s), B=%s (want %s)", gotA.MessageID, rA.MessageID, gotB.MessageID, rB.MessageID)
	}
}
