-- rehydrate.lua — warm a cold MT balance cache, unless a debit landed since the caller read the debit
-- counter (ADR-0022). The caller read that counter BEFORE the in-flight set and the durable balance, so a
-- value computed before a debit it did not see is refused instead of resurrecting the credit.
--
-- KEYS[1] = billing:balance:mt:{owner_type}:{owner_id}
-- KEYS[2] = billing:seq:mt:{owner_type}:{owner_id}
-- ARGV[1] = the counter the caller read ('' when absent)
-- ARGV[2] = the balance to cache
-- ARGV[3] = the cache TTL in ms
--
-- Returns 1 when the cache is warm (set now, or by someone else), 0 when a debit raced the caller.
if (redis.call('GET', KEYS[2]) or '') ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'NX', 'PX', ARGV[3])
return 1
