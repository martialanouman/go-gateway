package encoding

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/google/uuid"

	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
)

// A concatenated MO arrives as several deliver_sm, each a segment carrying a concatenation UDH
// (reference, total, sequence). The SMSC does not guarantee arrival order, and segments may be spread
// across pods, so reassembly buffers them in Redis keyed by the concatenation identity and completes
// the logical message only once every segment is present. The buffer is bounded: a group whose
// segments never all arrive evicts on a TTL, so an orphaned half-message cannot accumulate forever.
//
// The whole offer is ONE atomic Lua script (no Go read-modify-write on shared state): it stores the
// segment, evicts any segment older than the TTL, and — when the group is complete — returns the body
// assembled in sequence order and clears the buffer. Eviction is driven by a caller-supplied clock
// (passed as an argument), so a test advances time deterministically without sleeping; a PEXPIRE backstop
// also bounds a truly abandoned key in Redis.
const reassembleSrc = `
local key = KEYS[1]
local seq = ARGV[1]
local total = tonumber(ARGV[2])
local content = ARGV[3]
local now = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

redis.call('HSET', key, 'c:' .. seq, content, 't:' .. seq, now)
redis.call('HSETNX', key, 'total', total)
redis.call('PEXPIRE', key, ttl)
local storedTotal = tonumber(redis.call('HGET', key, 'total'))

-- Defence in depth against a malformed total (the caller also guards): a group of fewer than one
-- segment can never complete, and the completeness check below would false-positive on total 0.
if storedTotal < 1 then
	return false
end

-- Evict segments whose arrival is older than the TTL, and count the fresh ones present.
local present = 0
for i = 1, storedTotal do
	local ts = redis.call('HGET', key, 't:' .. i)
	if ts then
		if now - tonumber(ts) > ttl then
			redis.call('HDEL', key, 'c:' .. i, 't:' .. i)
		else
			present = present + 1
		end
	end
end

if present < storedTotal then
	return false
end

-- Complete: assemble in sequence order, then clear the buffer.
local parts = {}
for i = 1, storedTotal do
	parts[i] = redis.call('HGET', key, 'c:' .. i)
end
redis.call('DEL', key)
return table.concat(parts)
`

// Reassembler buffers the segments of concatenated MO messages in Redis and reassembles them.
type Reassembler struct {
	script *redisstore.Script
	ttl    time.Duration
	now    func() time.Time
}

// ReassemblerOption configures a Reassembler.
type ReassemblerOption func(*Reassembler)

// WithReassemblyClock overrides the clock used for segment ages (tests advance it to exercise TTL
// eviction without sleeping).
func WithReassemblyClock(now func() time.Time) ReassemblerOption {
	return func(r *Reassembler) { r.now = now }
}

// NewReassembler builds a Reassembler over a Redis client (any go-redis Scripter) with the given
// orphan TTL. A group whose segments do not all arrive within ttl is evicted.
func NewReassembler(client goredis.Scripter, ttl time.Duration, opts ...ReassemblerOption) *Reassembler {
	r := &Reassembler{
		script: redisstore.NewScript(client, reassembleSrc),
		ttl:    ttl,
		now:    time.Now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Offer submits one segment (content is the UDH-stripped payload) of a concatenated MO identified by
// (from, to, connectorID, ref). It returns the fully assembled body with complete=true once every
// segment of the group has arrived, or complete=false while any is still missing (the segment is
// buffered; the caller commits and waits for the rest). A redelivered segment (at-least-once) is
// idempotent: it overwrites its slot without changing the count. The body is handled as bytes only and
// never logged (invariant a).
func (r *Reassembler) Offer(ctx context.Context, from, to string, connectorID uuid.UUID, ref uint16, total, seq int, content []byte) (assembled []byte, complete bool, err error) {
	key := bufferKey(from, to, connectorID, ref)
	nowMs := r.now().UnixMilli()
	ttlMs := r.ttl.Milliseconds()

	res, err := r.script.Run(ctx, []string{key}, seq, total, content, nowMs, ttlMs).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil // the group is not complete yet
	}
	if err != nil {
		return nil, false, fmt.Errorf("encoding: reassemble offer: %w", err)
	}
	s, ok := res.(string)
	if !ok {
		return nil, false, fmt.Errorf("encoding: reassemble: unexpected result type %T", res)
	}
	return []byte(s), true, nil
}

// bufferKey identifies a concatenation group. The (from:to:connectorID:ref) tuple is the Redis Cluster
// hash tag ({...}) so the whole hash lives in one slot (the script is single-key, Cluster-safe), while
// distinct groups still spread across slots. ref is the 16-bit concatenation reference.
func bufferKey(from, to string, connectorID uuid.UUID, ref uint16) string {
	return fmt.Sprintf("moreasm:{%s:%s:%s:%d}", from, to, connectorID, ref)
}
