package antispam

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// Key prefixes namespace the shared anti-spam state so it never collides with other Redis data.
const (
	dupKeyPrefix    = "antispam:dup:"
	velKeyPrefix    = "antispam:vel:"
	repKeyPrefix    = "antispam:rep:"
	catmisKeyPrefix = "antispam:catmis:"
)

// countOnceSrc counts a message once in a bucket: the per-message marker and the increment move together,
// so a redelivery neither double-counts nor leaves a bucket without its TTL. KEYS[1]=bucket, KEYS[2]=marker;
// ARGV[1]=ttl_ms. Returns 1 when counted, 0 for a message already counted.
const countOnceSrc = `
if not redis.call('SET', KEYS[2], 1, 'NX', 'PX', ARGV[1]) then
  return 0
end
redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
`

// slidingWindowSrc is the atomic sliding-window counter (the golden rule forbids a read-modify-write
// from Go): trim events older than the window, add this event (its message_id as member), refresh the TTL,
// and return the count within the window. KEYS[1]=key; ARGV: now_ms, window_ms, member.
const slidingWindowSrc = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
redis.call('ZADD', key, now, ARGV[3])
redis.call('PEXPIRE', key, window)
return redis.call('ZCARD', key)
`

// recordSrc adds an event to a sliding-window set WITHOUT trimming to a specific window (a reader
// applies its own window), keeping the key alive for maxTTL. Used to count inbound MO into a source's
// velocity, whose window is decided by the MT rule that reads it. KEYS[1]=key; ARGV: now_ms, member,
// max_ttl_ms.
const recordSrc = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
`

// recordMaxTTL bounds how long a velocity set lingers so an idle source cannot leak keys. A velocity
// rule whose window exceeds this is dropped at load (compileVelocity): older events are already gone,
// so the count could not be trusted.
const recordMaxTTL = time.Hour

// RedisState is the shared anti-spam state: duplicate fingerprints, sliding-window velocity counters,
// and reputation scores. Every operation is a single-key op (Cluster-safe) and atomic. It satisfies
// the engine's StateStore.
type RedisState struct {
	rdb          *redis.Client
	slidingCount *redis.Script
	record       *redis.Script
	countOnce    *redis.Script
	now          func() time.Time
}

// NewRedisState builds the shared state over rdb. The Lua scripts are prepared once and run via
// EVALSHA (go-redis falls back to EVAL on first use).
func NewRedisState(rdb *redis.Client) *RedisState {
	return &RedisState{
		rdb:          rdb,
		slidingCount: redis.NewScript(slidingWindowSrc),
		record:       redis.NewScript(recordSrc),
		countOnce:    redis.NewScript(countOnceSrc),
		now:          time.Now,
	}
}

// Seen records a duplicate fingerprint for window and reports whether ANOTHER message already holds it
// (an atomic SET NX GET). The stored value is the message_id, never the body: a redelivered message finds
// its own id and is not its own duplicate.
func (s *RedisState) Seen(ctx context.Context, fingerprint string, messageID uuid.UUID, window time.Duration) (bool, error) {
	holder, err := s.rdb.SetArgs(ctx, dupKeyPrefix+fingerprint, messageID.String(), redis.SetArgs{Mode: "NX", Get: true, TTL: window}).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return holder != messageID.String(), nil
}

// Hit records one event under key in its sliding window and returns the count within the window
// (including this event). The count is compared against the rule's max by the caller. The member is the
// message_id, so a redelivered message counts once.
func (s *RedisState) Hit(ctx context.Context, key string, messageID uuid.UUID, window time.Duration) (int, error) {
	now := time.Now().UnixMilli()
	n, err := s.slidingCount.Run(ctx, s.rdb, []string{velKeyPrefix + key}, now, window.Milliseconds(), messageID.String()).Int()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Record adds one event under key WITHOUT trimming to a window (a reader trims to its own), for
// counting inbound MO into a source's velocity.
func (s *RedisState) Record(ctx context.Context, key string) error {
	now := time.Now().UnixMilli()
	member := strconv.FormatInt(now, 10) + ":" + uuid.NewString()
	return s.record.Run(ctx, s.rdb, []string{velKeyPrefix + key}, now, member, recordMaxTTL.Milliseconds()).Err()
}

// Reputation returns the reputation score recorded for source, and whether one exists. A missing
// score is not an error: the source is simply unscored (neutral).
func (s *RedisState) Reputation(ctx context.Context, source string) (int, bool, error) {
	score, err := s.rdb.Get(ctx, repKeyPrefix+source).Int()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return score, true, nil
}

// categoryMismatchWindow is how far back RecentCategoryMismatches looks: the current hourly bucket and the
// 23 before it. A bucket lives one hour longer, so the oldest one counted has not expired yet.
const (
	categoryMismatchWindow    = 24 * time.Hour
	categoryMismatchBucketTTL = categoryMismatchWindow + time.Hour
)

// CategoryMismatchKey is the counter a sender's category_mismatch matches land in for the hour of t.
func CategoryMismatchKey(customerID uuid.UUID, address string, t time.Time) string {
	return catmisKeyPrefix + customerID.String() + ":" + t.UTC().Format("2006010215") + ":" + address
}

// CountCategoryMismatch adds messageID to the sender's current hourly bucket, once: a redelivered message
// is not counted again (counted is false).
func (s *RedisState) CountCategoryMismatch(ctx context.Context, messageID, customerID uuid.UUID, address string) (bool, error) {
	keys := []string{CategoryMismatchKey(customerID, address, s.now()), catmisKeyPrefix + "seen:" + messageID.String()}
	n, err := s.countOnce.Run(ctx, s.rdb, keys, categoryMismatchBucketTTL.Milliseconds()).Int()
	return n == 1, err
}

// RecentCategoryMismatches returns, in the order of senders, the matches of the current hourly bucket and
// the 23 before it, read in a single MGET however many senders are asked for.
func (s *RedisState) RecentCategoryMismatches(ctx context.Context, senders []cp.SenderAddress) ([]int, error) {
	if len(senders) == 0 {
		return []int{}, nil
	}
	buckets := int(categoryMismatchWindow / time.Hour)
	now := s.now()
	keys := make([]string, 0, len(senders)*buckets)
	for _, sa := range senders {
		for h := range buckets {
			keys = append(keys, CategoryMismatchKey(sa.CustomerID, sa.Address, now.Add(-time.Duration(h)*time.Hour)))
		}
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]int, len(senders))
	for i, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue
		}
		n, err := strconv.Atoi(str)
		if err != nil {
			return nil, fmt.Errorf("antispam: category mismatch bucket %d: %w", i, err)
		}
		out[i/buckets] += n
	}
	return out, nil
}
