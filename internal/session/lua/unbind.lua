-- unbind.lua — remove a session token (graceful unbind or connection loss).
--
-- KEYS[1] = sess:{account_id}
-- ARGV[1] = member  "pod_id:bind_id"
--
-- Returns the number of members removed (1 if the session existed, 0 otherwise).

return redis.call('ZREM', KEYS[1], ARGV[1])
