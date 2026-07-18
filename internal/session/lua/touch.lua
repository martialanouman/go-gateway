-- touch.lua — refresh a session's TTL on enquire_link supervision.
--
-- KEYS[1] = sess:{account_id}
-- ARGV[1] = member   "pod_id:bind_id"
-- ARGV[2] = now      unix seconds
-- ARGV[3] = expiry   new unix-seconds score (now + ttl)
-- ARGV[4] = key_ttl  seconds; whole-key EXPIRE
--
-- Returns 1 if the session was present AND still live, 0 otherwise. Expiry sweeping is lazy (only
-- Bind/Lookup sweep), so a member can linger in the set past its score; Touch must not resurrect such
-- a lapsed session — it checks the score against now and drops a lapsed member instead of refreshing
-- it, so a bind whose enquire_link stopped stays released.

local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) > tonumber(ARGV[2]) then
  redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[4])
  return 1
end
redis.call('ZREM', KEYS[1], ARGV[1])
return 0
