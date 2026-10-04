-- repair.lua — a duplicate found a live hold and is about to write its missing durable reserve (step-286).
-- Only while the hold still stands: once the original attempt undid it, the cache no longer carries this
-- debit and a durable one would leave the cache above the ledger. In the same step it covers the debit with
-- its own in-flight field and marks the hold, so the original attempt's undo is either before this script
-- (the caller reserves anew) or after it (the undo sees the mark and refunds nothing).
--
-- KEYS[1] = billing:reservation:{message_id}
-- KEYS[2] = billing:inflight:mt:{owner_type}:{owner_id}
-- KEYS[3] = billing:repaired:{message_id}
-- ARGV[1] = the repair's in-flight field
-- ARGV[2] = "credits:now_ms"
-- ARGV[3] = the mark's TTL in ms
--
-- Returns 1 when covered and marked, 0 when the hold is gone.
if not redis.call('GET', KEYS[1]) then
  return 0
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('SET', KEYS[3], 1, 'PX', ARGV[3])
return 1
