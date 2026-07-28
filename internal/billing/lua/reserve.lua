-- reserve.lua — atomic MT credit reserve (step-142). Holds `credits` against the owner's cached balance
-- for a message_id, idempotent by the reservation key. The whole check-and-debit is ONE script so two
-- concurrent reserves of the same owner can never both spend the last credit (the golden rule forbids a
-- Go read-modify-write on shared balance state).
--
-- KEYS[1] = billing:balance:{direction}:{owner_type}:{owner_id}  (integer credit balance cache)
-- KEYS[2] = billing:reservation:{message_id}                     (short-TTL hold; stores the reserved amount)
-- ARGV[1] = credits    the segments to reserve (> 0)
-- ARGV[2] = has_floor  1 if a minimum-balance floor applies, else 0 (an explicit flag, never a sentinel)
-- ARGV[3] = floor      the minimum the balance may reach (e.g. 0 for strict prepaid, negative for overdraft)
-- ARGV[4] = ttl_ms     the reservation hold's TTL
--
-- Returns one of:
--   {"reserved", new_balance}   the hold was placed and the balance debited
--   {"held", reserved_credits}  the message_id was ALREADY reserved (idempotent) — the caller compares the
--                               amount (a retry with a different amount is an error, never an overwrite)
--   {"cold"}                    the balance cache is absent — the caller rehydrates from Postgres and retries
--   {"insufficient", balance}   the balance would fall below the floor — NO mutation, no hold placed
local bkey = KEYS[1]
local rkey = KEYS[2]
local credits = tonumber(ARGV[1])
local has_floor = tonumber(ARGV[2])
local floor = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

-- Idempotent replay: an existing hold means this message_id was already reserved. Return the stored
-- amount without re-debiting; the balance is unchanged.
local held = redis.call('GET', rkey)
if held then
  return {'held', tonumber(held)}
end

-- A cold cache must be rehydrated from the durable authority (Postgres) before we can decide — never
-- pass an unverified credit (fail-closed).
local balance = redis.call('GET', bkey)
if not balance then
  return {'cold'}
end
balance = tonumber(balance)

if has_floor == 1 and (balance - credits) < floor then
  return {'insufficient', balance}
end

balance = balance - credits
redis.call('SET', bkey, balance, 'KEEPTTL')
redis.call('SET', rkey, credits, 'PX', ttl)
return {'reserved', balance}
