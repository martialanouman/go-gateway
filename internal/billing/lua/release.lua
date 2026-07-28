-- release.lua — refund an MT reservation whose message failed before it was sent (step-142). Adds the
-- held credits back to the cached balance and clears the hold, atomically.
--
-- KEYS[1] = billing:balance:{direction}:{owner_type}:{owner_id}  (integer credit balance cache)
-- KEYS[2] = billing:reservation:{message_id}                     (the hold to refund)
--
-- Returns one of:
--   {"released", new_balance, credits}  the held credits were added back to the balance
--   {"cold", credits}            the hold was live but the balance cache had lapsed — the caller refunds
--                                DURABLY and lets the cache rehydrate from Postgres (INCRBY-from-absent
--                                would fabricate a balance from 0, so we never touch the balance here)
--   {"no_reservation"}           no live hold — already captured/released, or the hold's TTL lapsed; the
--                                caller disambiguates via the durable ledger
local held = redis.call('GET', KEYS[2])
if not held then
  return {'no_reservation'}
end
held = tonumber(held)

local balance = redis.call('GET', KEYS[1])
if not balance then
  redis.call('DEL', KEYS[2])
  return {'cold', held}
end
balance = tonumber(balance) + held
redis.call('SET', KEYS[1], balance, 'KEEPTTL')
redis.call('DEL', KEYS[2])
return {'released', balance, held}
