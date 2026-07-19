-- incr_expire.lua — atomic increment of a bind-failure counter with a sliding window.
--
-- KEYS[1] = bindfail counter key (per system_id or per source IP)
-- ARGV[1] = window seconds; the EXPIRE on every increment makes the window slide from the last
--           failure, so a burst of failures keeps the counter (and the lockout) alive, while an
--           attacker who backs off for the whole window is forgiven.
--
-- Returns the new counter value. INCR then EXPIRE in one script keeps the pair atomic — a counter is
-- never left without a TTL — and honours the golden rule: no read-modify-write from Go.

local n = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[1])
return n
