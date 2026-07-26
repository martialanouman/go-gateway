-- Atomic token bucket (step-084). Refills at `rate` tokens per second up to `capacity`, then consumes
-- `cost` tokens if enough are available. The whole refill-and-consume is ONE script so the decision is
-- atomic under concurrency: the golden rule forbids a Go read-modify-write on shared rate state, and
-- two concurrent callers can never both spend the last token.
--
-- State is a hash {tokens, ts} under a single key (KEYS[1]), Cluster-safe.
-- ARGV: now_ms, rate_per_sec, capacity, cost, ttl_ms. Returns {allowed (0|1), remaining_tokens}.
--
-- NOTE: Redis converts a Lua number to an INTEGER when it is passed as a command argument, dropping the
-- fraction. tokens is fractional (it refills continuously), so it is stored via tostring() and parsed
-- back with tonumber() — otherwise partial refills would be silently truncated away.

local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
	tokens = capacity
	ts = now
end

-- Refill for the elapsed time (never negative under clock skew), capped at capacity.
local elapsed = now - ts
if elapsed < 0 then
	elapsed = 0
end
tokens = math.min(capacity, tokens + (elapsed / 1000.0) * rate)

local allowed = 0
if tokens >= cost then
	tokens = tokens - cost
	allowed = 1
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', key, ttl)
return {allowed, math.floor(tokens)}
