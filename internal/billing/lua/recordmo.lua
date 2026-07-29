-- recordmo.lua — atomic MO meter debit (step-143). The MO (mobile-originated, return path) counter is a
-- POSTPAID METER, distinct from the MT balance: it accrues usage as a growing NEGATIVE balance and NEVER
-- blocks anything (the MO was already delivered). Each MO debits `credits`; accrual STOPS at
-- mo_billing_floor (full-then-stop: the message that crosses accrues in full, later ones are not accrued).
-- The whole check-and-debit is ONE script so N concurrent MOs overshoot the floor by at most one message.
--
-- KEYS[1] = billing:balance:mo:{owner_type}:{owner_id}   (integer MO meter cache)
-- KEYS[2] = billing:mo-seen:{message_id}                 (idempotency guard, short-TTL)
-- ARGV[1] = credits      the MO cost to accrue (> 0)
-- ARGV[2] = has_floor    1 if a floor applies, else 0 (an explicit flag, never a sentinel)
-- ARGV[3] = floor        the most-negative the meter may reach before accrual stops
-- ARGV[4] = seen_ttl_ms  TTL of the seen-key (the first-layer idempotency window)
--
-- Returns one of:
--   {"cold"}                          the cache is absent — caller rehydrates from Postgres and retries
--   {"duplicate", balance}            this message_id was already accrued (seen-key hit) — no-op
--   {"stopped", balance}              the meter is already at/below the floor — NOT accrued (accrual stopped)
--   {"charged", new_balance, crossed} accrued in full; crossed=1 iff this debit is the one that reached the
--                                     floor (old > floor and new <= floor) — the caller alerts exactly once
local bkey, skey = KEYS[1], KEYS[2]
local credits = tonumber(ARGV[1])
local has_floor = tonumber(ARGV[2])
local floor = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local balance = redis.call('GET', bkey)
if not balance then
  return {'cold'}
end
balance = tonumber(balance)

-- Accrual stops at the floor. An already-floored meter does NOT accrue this MO, and is deliberately NOT
-- marked seen — so a later top-up back above the floor lets a redelivery accrue it.
if has_floor == 1 and balance <= floor then
  return {'stopped', balance}
end

-- First-layer idempotency, checked AFTER the floor-stop so a suppressed MO is never marked seen. A replay
-- within the window is a no-op; a replay past the TTL falls through to the durable idempotency guard.
if redis.call('SET', skey, '1', 'NX', 'PX', ttl) == false then
  return {'duplicate', balance}
end

local new = balance - credits
redis.call('SET', bkey, new, 'KEEPTTL')
local crossed = 0
if has_floor == 1 and new <= floor then
  crossed = 1
end
return {'charged', new, crossed}
