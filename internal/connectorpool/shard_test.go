package connectorpool

import (
	"testing"

	"github.com/google/uuid"
)

// TestShardIndexDeterministic: the same key always maps to the same shard (so a message's segments,
// which share the key, always ride the same bind), keys spread across the pool, and n<=1 collapses to
// shard 0.
func TestShardIndexDeterministic(t *testing.T) {
	key := uuid.New()
	kb := key[:]
	first := shardIndex(kb, 8)
	for i := 0; i < 100; i++ {
		if got := shardIndex(kb, 8); got != first {
			t.Fatalf("shardIndex not stable: call %d = %d, want %d", i, got, first)
		}
	}
	if got := shardIndex(kb, 1); got != 0 {
		t.Errorf("shardIndex(_, 1) = %d, want 0", got)
	}
	if got := shardIndex(kb, 0); got != 0 {
		t.Errorf("shardIndex(_, 0) = %d, want 0 (guard)", got)
	}

	// Every shard is in range and, over many keys, the pool is used (not all collapsing to one bind).
	const n = 4
	hits := make([]int, n)
	for i := 0; i < 400; i++ {
		id := uuid.New()
		sh := shardIndex(id[:], n)
		if sh < 0 || sh >= n {
			t.Fatalf("shard %d out of range [0,%d)", sh, n)
		}
		hits[sh]++
	}
	for sh, h := range hits {
		if h == 0 {
			t.Errorf("shard %d received nothing over 400 keys — sharding is not spreading", sh)
		}
	}
}
