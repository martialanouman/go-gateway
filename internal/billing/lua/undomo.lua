-- undomo.lua — reverse a speculative recordmo.lua meter debit whose durable side did not stick (step-143).
-- Adds `credits` back to the MO meter ONLY if the balance key still exists (INCRBY preserves its TTL); if
-- the key has since expired, it is deliberately NOT recreated — a phantom, TTL-less, positive meter value
-- would never self-heal, whereas an absent key rehydrates from the durable authority on the next MO. The
-- seen-key is cleared either way so a legitimate retry can re-accrue.
--
-- KEYS[1] = billing:balance:mo:{owner_type}:{owner_id}
-- KEYS[2] = billing:mo-seen:{message_id}
-- ARGV[1] = credits  the amount to add back (> 0)
if redis.call('EXISTS', KEYS[1]) == 1 then
  redis.call('INCRBY', KEYS[1], tonumber(ARGV[1]))
end
redis.call('DEL', KEYS[2])
return 1
