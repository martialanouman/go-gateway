package antispam

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Key prefixes namespace the shared anti-spam state so it never collides with other Redis data.
const (
	dupKeyPrefix = "antispam:dup:"
	velKeyPrefix = "antispam:vel:"
	repKeyPrefix = "antispam:rep:"
)

// slidingWindowSrc is the atomic sliding-window counter (the golden rule forbids a read-modify-write
// from Go): trim events older than the window, add this event with a unique member, refresh the TTL,
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
}

// NewRedisState builds the shared state over rdb. The Lua scripts are prepared once and run via
// EVALSHA (go-redis falls back to EVAL on first use).
func NewRedisState(rdb *redis.Client) *RedisState {
	return &RedisState{
		rdb:          rdb,
		slidingCount: redis.NewScript(slidingWindowSrc),
		record:       redis.NewScript(recordSrc),
	}
}

// Seen records a duplicate fingerprint for window and reports whether it was ALREADY present (an
// atomic SET NX EX). The stored value is a constant marker, never the body.
func (s *RedisState) Seen(ctx context.Context, fingerprint string, window time.Duration) (bool, error) {
	set, err := s.rdb.SetNX(ctx, dupKeyPrefix+fingerprint, "1", window).Result()
	if err != nil {
		return false, err
	}
	return !set, nil
}

// Hit records one event under key in its sliding window and returns the count within the window
// (including this event). The count is compared against the rule's max by the caller.
func (s *RedisState) Hit(ctx context.Context, key string, window time.Duration) (int, error) {
	now := time.Now().UnixMilli()
	member := strconv.FormatInt(now, 10) + ":" + uuid.NewString()
	n, err := s.slidingCount.Run(ctx, s.rdb, []string{velKeyPrefix + key}, now, window.Milliseconds(), member).Int()
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
