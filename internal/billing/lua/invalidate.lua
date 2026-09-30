-- invalidate.lua — drop balance caches after a durable write outside reserve.lua lowered the balance
-- (ADR-0022). Bumping the debit counter with the delete refuses a rehydration computed before the write.
--
-- KEYS = pairs of (billing:balance:{…}, billing:seq:{…})
for i = 1, #KEYS, 2 do
  redis.call('INCR', KEYS[i + 1])
  redis.call('DEL', KEYS[i])
end
return 1
