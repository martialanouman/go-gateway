package cancel

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// flagTTL bounds how long a cancel intent lives in Redis. It must outlast the longest a message can
// sit queued before dispatch (its validity_period); 72h covers the SMS maximum. Past it the message
// is no longer routable, so a stale flag would gate nothing — the TTL just reclaims the key.
const flagTTL = 72 * time.Hour

// RedisFlags is the shared cancel-intent store: the Canceller Marks a message, the connector pool
// checks Exists before submit_sm. The key embeds the message_id as its Redis Cluster hash tag, so
// every operation touches a single slot (never a multi-key op).
type RedisFlags struct {
	rdb *redis.Client
}

// NewRedisFlags builds a flag store over rdb.
func NewRedisFlags(rdb *redis.Client) *RedisFlags {
	return &RedisFlags{rdb: rdb}
}

// key scopes the flag under the message_id, which doubles as the Cluster hash tag.
func key(id uuid.UUID) string { return "cancel:{" + id.String() + "}" }

// Mark records the cancel intent with a bounded TTL. A repeat Mark simply refreshes it.
func (f *RedisFlags) Mark(ctx context.Context, id uuid.UUID) error {
	if err := f.rdb.Set(ctx, key(id), "1", flagTTL).Err(); err != nil {
		return fmt.Errorf("cancel: mark %s: %w", id, err)
	}
	return nil
}

// Exists reports whether a cancel intent is recorded for id. A missing key is not an error.
func (f *RedisFlags) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	n, err := f.rdb.Exists(ctx, key(id)).Result()
	if err != nil {
		return false, fmt.Errorf("cancel: check %s: %w", id, err)
	}
	return n > 0, nil
}
