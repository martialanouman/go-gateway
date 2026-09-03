package exact

import (
	"context"
	"errors"
	"fmt"
	"slices"

	goredis "github.com/redis/go-redis/v9"
)

// invalidateChunk bounds one pipeline round trip. An MNP import invalidates up to 10 000 numbers at
// once; sending them as one buffer would hold the whole batch in memory on both ends for no gain.
const invalidateChunk = 1000

// RedisPipeliner is the write surface the invalidator needs. A *goredis.Client satisfies it, and so
// does a *goredis.ClusterClient — which groups a pipeline's commands per node, the reason invalidation
// goes through a pipeline rather than one multi-key DEL.
type RedisPipeliner interface {
	Pipelined(ctx context.Context, fn func(goredis.Pipeliner) error) ([]goredis.Cmder, error)
}

// Invalidator clears cached exact routes. It is the control plane's ONLY write to the data-plane cache
// (step-250e): the Admin API says "what you believe about this number is no longer true" and never says
// what the new truth is. That asymmetry is what keeps exactroute:{msisdn} a cache — rebuildable from
// Postgres by the resolver at any time — rather than a second source of truth that a Redis failover, a
// FLUSHALL or a resharding could silently lose.
//
// Callers invalidate AFTER their durable commit. A crash in between leaves a stale key for at most the
// resolver's TTL; the reverse order would let a reader repopulate the pre-commit value and pin it.
type Invalidator struct{ rdb RedisPipeliner }

// NewInvalidator builds the cache invalidator over a Redis client.
func NewInvalidator(rdb RedisPipeliner) *Invalidator { return &Invalidator{rdb: rdb} }

// Invalidate drops the cached route of every listed MSISDN. The numbers must already be in the
// canonical E.164 digits-only form the resolver keys on. Deleting an absent key is a no-op, so this is
// idempotent and safe to retry — and safe on create, where there is usually nothing to drop.
func (i *Invalidator) Invalidate(ctx context.Context, msisdns ...string) error {
	var failures []error
	for chunk := range slices.Chunk(msisdns, invalidateChunk) {
		_, err := i.rdb.Pipelined(ctx, func(p goredis.Pipeliner) error {
			for _, m := range chunk {
				// One key per command: the {msisdn} hash tag puts each number on its own cluster slot,
				// so a multi-key DEL would be a CROSSSLOT error in the clustered deployment the tag
				// exists for. The pipeline is what makes this one round trip anyway.
				p.Del(ctx, redisKey(m))
			}
			return nil
		})
		// Keep going. Abandoning the remaining chunks on the first failure would leave thousands of an
		// import's keys pointing at the previous carrier for a whole TTL — including keys whose Redis
		// node was healthy, since a cluster pipeline fails per node. DEL is idempotent, so a chunk that
		// partly applied costs nothing to retry, and every failure is still reported.
		if err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("exact: invalidate cache: %w", errors.Join(failures...))
	}
	return nil
}
