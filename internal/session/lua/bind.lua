-- bind.lua — atomic bind against the account's max_sessions quota (invariant d).
--
-- KEYS[1] = sess:{account_id}   sorted set, member = "pod_id:bind_id", score = expiry (unix seconds)
-- ARGV[1] = member              "pod_id:bind_id"
-- ARGV[2] = max_sessions        integer quota (count >= max rejects; 0 admits nothing)
-- ARGV[3] = now                 unix seconds, used to sweep expired members first
-- ARGV[4] = expiry              unix seconds score for this member (now + ttl)
-- ARGV[5] = key_ttl             seconds; whole-key EXPIRE so an idle account key eventually vanishes
--
-- Returns {accepted, active} where accepted is 1|0 and active is the session count for the account.
-- The count-check-insert is a single script so the quota holds under concurrent binds.

local key    = KEYS[1]
local member = ARGV[1]
local max    = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local expiry = tonumber(ARGV[4])
local keyTTL = tonumber(ARGV[5])

-- Lazy sweep: drop members whose expiry has passed before counting.
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- A member that is already registered is a rebind: refresh its expiry without counting against the
-- quota (it already holds a slot).
if not redis.call('ZSCORE', key, member) then
  local count = redis.call('ZCARD', key)
  if count >= max then
    return {0, count}
  end
end

redis.call('ZADD', key, expiry, member)
redis.call('EXPIRE', key, keyTTL)
return {1, redis.call('ZCARD', key)}
