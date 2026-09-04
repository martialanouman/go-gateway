-- Atomic per-bind status publish + derived connector load (step-260d). Records one sub-bind's link
-- status and in_flight, sweeps the entries whose publish time has aged past the bind TTL, and derives
-- connectorload:{id} — the connector-wide in-flight gauge least_loaded reads (Appendix B) — as the sum
-- of the LIVE entries. The sum is a read-modify-write, so it lives here, never in Go (golden rule).
--
-- KEYS[1] = connector:binds:{connector_id}  (HASH: field "{pod_id}:{bind_index}" -> JSON BindEntry)
-- KEYS[2] = connectorload:{connector_id}    (STRING: derived in-flight sum, read by the router)
-- ARGV[1] = field ("{pod_id}:{bind_index}")   ARGV[2] = JSON BindEntry for this sub-bind
-- ARGV[3] = now (ms)   ARGV[4] = bind TTL (ms; per-field staleness AND key expiry)
-- Returns the derived sum.

local binds, loadkey = KEYS[1], KEYS[2]
local field, entry = ARGV[1], ARGV[2]
local now, ttl = tonumber(ARGV[3]), tonumber(ARGV[4])

redis.call('HSET', binds, field, entry)

-- Sweep stale or unreadable entries (a shrunk bind, a crashed pod); sum the live ones. A zero/absent
-- ts is an unstamped legacy entry, treated as fresh — the same rule Read applies.
local all = redis.call('HGETALL', binds)
local sum = 0
for i = 1, #all, 2 do
	local f, v = all[i], all[i + 1]
	local ok, e = pcall(cjson.decode, v)
	if not ok or type(e) ~= 'table' then
		redis.call('HDEL', binds, f)
	else
		local ts = tonumber(e['ts']) or 0
		if ts ~= 0 and (now - ts) > ttl then
			redis.call('HDEL', binds, f)
		else
			sum = sum + (tonumber(e['in_flight']) or 0)
		end
	end
end

redis.call('SET', loadkey, sum, 'PX', ttl)
redis.call('PEXPIRE', binds, ttl)
return sum
