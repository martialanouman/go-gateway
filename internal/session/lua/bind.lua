-- bind.lua — atomic bind against the account's max_sessions quota (invariant d).
--
-- KEYS[1] = sess:{account_id}   sorted set, member = "pod_id:bind_id", score = expiry (unix seconds)
-- KEYS[2] = sess:{account_id}:meta  hash, field = bind_id, value = the bind's JSON description
-- ARGV[1] = member              "pod_id:bind_id"
-- ARGV[2] = max_sessions        integer quota (count >= max rejects; 0 admits nothing)
-- ARGV[3] = now                 unix seconds, used to sweep expired members first
-- ARGV[4] = expiry              unix seconds score for this member (now + ttl)
-- ARGV[5] = key_ttl             seconds; whole-key EXPIRE so an idle account key eventually vanishes
-- ARGV[6] = bind_id
-- ARGV[7] = meta                JSON description; HSETNX, so a refresh keeps the first connected_at
--
-- Returns {accepted, active} where accepted is 1|0 and active is the session count for the account.
-- The count-check-insert is a single script so the quota holds under concurrent binds.

local key    = KEYS[1]
local member = ARGV[1]
local max    = tonumber(ARGV[2])
local now    = tonumber(ARGV[3])
local expiry = tonumber(ARGV[4])
local keyTTL = tonumber(ARGV[5])

-- Lazy sweep: drop members whose expiry has passed before counting, and their descriptions with them —
-- the key never expires while the account holds one live bind.
for _, m in ipairs(redis.call('ZRANGEBYSCORE', key, '-inf', now)) do
  redis.call('HDEL', KEYS[2], string.sub(m, string.find(m, ':', 1, true) + 1))
end
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
redis.call('HSETNX', KEYS[2], ARGV[6], ARGV[7])
redis.call('EXPIRE', KEYS[2], keyTTL)
return {1, redis.call('ZCARD', key)}
