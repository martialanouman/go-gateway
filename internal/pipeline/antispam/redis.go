package antispam

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// dupKeyPrefix namespaces the duplicate fingerprints so they never collide with other Redis state.
const dupKeyPrefix = "antispam:dup:"

// RedisDuplicateChecker records message fingerprints in Redis with a TTL. Each check is a single-key
// atomic SET NX EX, so Cluster never sees a multi-key op and two concurrent submissions of the same
// message race cleanly: exactly one wins the SET and is treated as the original.
type RedisDuplicateChecker struct {
	rdb *redis.Client
}

// NewRedisDuplicateChecker builds a duplicate checker over rdb.
func NewRedisDuplicateChecker(rdb *redis.Client) *RedisDuplicateChecker {
	return &RedisDuplicateChecker{rdb: rdb}
}

// Seen records fingerprint for window and reports whether it was ALREADY present. The value stored is
// a constant marker, never the body — the fingerprint is already a one-way hash. SET NX returns true
// when the key was newly set (a first sighting), so "seen" is its negation.
func (c *RedisDuplicateChecker) Seen(ctx context.Context, fingerprint string, window time.Duration) (bool, error) {
	set, err := c.rdb.SetNX(ctx, dupKeyPrefix+fingerprint, "1", window).Result()
	if err != nil {
		return false, err
	}
	return !set, nil
}
