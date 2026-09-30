-- invalidate.lua — drop balance caches after an admin durable write (ADR-0022). Bumping the MT debit
-- counter with the delete refuses a rehydration computed before a write that lowered the balance.
--
-- KEYS[1..n] = billing:balance:{…} to drop, then billing:seq:mt:{…} to bump
-- ARGV[1]    = n
local n = tonumber(ARGV[1])
for i = 1, #KEYS do
  if i <= n then
    redis.call('DEL', KEYS[i])
  else
    redis.call('INCR', KEYS[i])
  end
end
return 1
