package dlrmap_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// storedMapping mirrors the JSON value the store writes, so the test reads it back the way step-044
// will.
type storedMapping struct {
	MessageID string `json:"message_id"`
	TraceID   string `json:"trace_id"`
}

// TestRedisMapPutStoresMappingWithTTL: a Put writes the {message_id, trace_id} value under the
// composite key with a TTL derived from the validity_period.
func TestRedisMapPutStoresMappingWithTTL(t *testing.T) {
	rdb := redistest.Client(t)
	store := dlrmap.NewRedisMap(rdb)
	ctx := context.Background()

	connectorID, messageID, traceID := uuid.New(), uuid.New(), uuid.New()
	smscID := "00000000000000ab"
	vp := "000001000000000R" // 1 day, relative

	if err := store.Put(ctx, connectorID, smscID, messageID, traceID, &vp); err != nil {
		t.Fatalf("Put: %v", err)
	}

	k := "dlrmap:{" + connectorID.String() + ":" + smscID + "}"
	val, err := rdb.Get(ctx, k).Result()
	if err != nil {
		t.Fatalf("Get %s: %v", k, err)
	}
	var got storedMapping
	if err := json.Unmarshal([]byte(val), &got); err != nil {
		t.Fatalf("unmarshal %q: %v", val, err)
	}
	if got.MessageID != messageID.String() || got.TraceID != traceID.String() {
		t.Errorf("value = %+v, want message_id %s / trace_id %s", got, messageID, traceID)
	}

	ttl, err := rdb.TTL(ctx, k).Result()
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	// 1 day + 1h margin = 25h; allow slack for the round trip.
	if ttl < 24*time.Hour || ttl > 26*time.Hour {
		t.Errorf("TTL = %v, want ~25h (validity 1d + margin)", ttl)
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
	msgA, msgB := uuid.New(), uuid.New()

	if err := store.Put(ctx, connA, smscID, msgA, uuid.New(), nil); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := store.Put(ctx, connB, smscID, msgB, uuid.New(), nil); err != nil {
		t.Fatalf("Put B: %v", err)
	}

	read := func(conn uuid.UUID) string {
		k := "dlrmap:{" + conn.String() + ":" + smscID + "}"
		val, err := rdb.Get(ctx, k).Result()
		if err != nil {
			t.Fatalf("Get %s: %v", k, err)
		}
		var m storedMapping
		if err := json.Unmarshal([]byte(val), &m); err != nil {
			t.Fatalf("unmarshal %q: %v", val, err)
		}
		return m.MessageID
	}
	if read(connA) != msgA.String() {
		t.Errorf("connector A mapping = %s, want %s", read(connA), msgA)
	}
	if read(connB) != msgB.String() {
		t.Errorf("connector B mapping = %s, want %s", read(connB), msgB)
	}
}
