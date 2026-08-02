package connectorpool_test

import (
	"hash/fnv"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/smscsim"
)

// balancedMessageIDs returns count message ids spread EXACTLY evenly across `shards` bind shards, using
// the pool's own key hash (FNV-32a of the id bytes % shards, mirroring shardIndex). This removes the
// throughput test's dependency on a lucky random split — every bind gets count/shards records, so the
// wall-clock is deterministic (max-shard × per-submit latency), not a multinomial tail risk.
func balancedMessageIDs(t *testing.T, count, shards int) []uuid.UUID {
	t.Helper()
	if count%shards != 0 {
		t.Fatalf("balancedMessageIDs: count %d not divisible by shards %d", count, shards)
	}
	per := count / shards
	filled := make([]int, shards)
	ids := make([]uuid.UUID, 0, count)
	for len(ids) < count {
		id := uuid.New()
		h := fnv.New32a()
		_, _ = h.Write(id[:])
		s := int(h.Sum32() % uint32(shards))
		if filled[s] < per {
			filled[s]++
			ids = append(ids, id)
		}
	}
	return ids
}

// TestSimBindPoolThroughput is the step-130c acceptance scenario (plan §12): bind_pool_size=4 raises a
// connector's throughput. Each bind submits synchronously (b.Submit waits for the resp), and the batch
// handler processes one bind's records sequentially, so against a slow-carrier (2s per submit) a pool of 4
// binds — fed a burst balanced 2-per-bind — clears 8 messages in ~2 rounds (~4-5s), while a single bind
// would need 8×2s = 16s. The 10s bound sits between the two, so a serialised (pool-of-1) pool fails it and
// the parallel pool passes. The burst is balanced deterministically (no random-split flake). Segment→
// single-bind ordering is the FNV sharding, unit-tested in shard_test.go.
func TestSimBindPoolThroughput(t *testing.T) {
	const slowMs, poolSize, burst = 2000, 4, 8
	sim := smscsim.Launch(t, smscsim.SlowCarrierConfig("slow", "pw", slowMs))
	pool := startSimPool(t, simPoolConfig{
		BindAddr: sim.SMPPAddr, SystemID: "slow", Password: "pw", BindPoolSize: poolSize,
		ResponseTimeout: 5 * time.Second, // above the 2s latency, so a healthy submit is NOT timed out
	})

	sim.WaitBindCount(t, "carrier", poolSize, 20*time.Second) // all four binds established
	waitGroupStable(t, pool.group, 1, 15*time.Second)

	ids := make([]routedIdent, 0, burst)
	for _, mid := range balancedMessageIDs(t, burst, poolSize) { // 2 per bind
		ids = append(ids, pool.injectRoutedWithID(t, mid, nil))
	}

	// ALL messages must reach enroute within ONE 12s window from injection — a shared deadline, so a
	// serialised (single-bind, ~16s) pool fails while the balanced 4-bind pool (~4-5s, or ~8s if franz-go
	// splits the burst into two poll batches) passes with margin even on a slow CI.
	allEnrouteBy := time.Now().Add(12 * time.Second)
	for _, id := range ids {
		remaining := time.Until(allEnrouteBy)
		if remaining <= 0 {
			remaining = time.Millisecond
		}
		pool.waitOutcome(t, id, clickhouse.StatusEnroute, remaining)
	}
}
