-- reserve.lua — atomic reserve-or-read for the REST Idempotency-Key window.
--
-- KEYS[1] = idem:{account_id}:<key>   hash: {body_hash, response, state}
-- ARGV[1] = body_hash                 SHA-256 hex of the normalized request body
-- ARGV[2] = response                  the 202 body to replay (ids only, never the message body)
-- ARGV[3] = ttl                       seconds; the 24 h window EXPIRE set on first reservation
--
-- Returns one of:
--   {"reserved"}              the caller won the slot and MUST publish, then Finalize (or Release on failure)
--   {"conflict"}             the key exists with a different body_hash (→ 409 idempotency_conflict)
--   {"pending", response}    a concurrent caller reserved but has not confirmed the publish yet
--   {"done", response}       the original result, safe to replay
--
-- The reserve-or-read is a single script so, under concurrent submits of the same key, exactly one
-- caller is told "reserved" (never a read-modify-write from Go).

local key      = KEYS[1]
local hash     = ARGV[1]
local response = ARGV[2]
local ttl      = tonumber(ARGV[3])

if redis.call('EXISTS', key) == 0 then
  redis.call('HSET', key, 'body_hash', hash, 'response', response, 'state', 'pending')
  redis.call('EXPIRE', key, ttl)
  return {'reserved'}
end

if redis.call('HGET', key, 'body_hash') ~= hash then
  return {'conflict'}
end

return {redis.call('HGET', key, 'state'), redis.call('HGET', key, 'response')}
