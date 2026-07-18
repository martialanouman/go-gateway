-- lookup.lua — list an account's live sessions, sweeping expired ones first.
--
-- KEYS[1] = sess:{account_id}
-- ARGV[1] = now  unix seconds
--
-- Returns the live members ("pod_id:bind_id"), so a caller never sees a session whose TTL has lapsed.

redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
return redis.call('ZRANGE', KEYS[1], 0, -1)
