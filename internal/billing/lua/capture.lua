-- capture.lua — commit an MT reservation once the SMSC accepted the message (step-142). The balance was
-- already debited at reserve, so a capture only clears the hold; it never touches the balance. It is
-- atomic so a double capture (double delivery) cannot clear a hold twice.
--
-- KEYS[1] = billing:reservation:{message_id}                     (the hold to consume)
-- KEYS[2] = billing:balance:{direction}:{owner_type}:{owner_id}  (read only, for the ledger's balance_after)
--
-- Returns one of:
--   {"captured", credits, balance}   the hold was consumed; balance is the current cached balance, or ""
--                                    if the cache is cold (the caller recovers balance_after from the ledger)
--   {"no_reservation"}               no live hold — either already captured/released, or the hold's TTL
--                                    lapsed; the caller disambiguates via the durable ledger
local held = redis.call('GET', KEYS[1])
if not held then
  return {'no_reservation'}
end
redis.call('DEL', KEYS[1])
local balance = redis.call('GET', KEYS[2])
return {'captured', tonumber(held), balance or ''}
