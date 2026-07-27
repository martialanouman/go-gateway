-- Atomic multi-pod breaker aggregation (step-122). Records one sub-bind's current state with a
-- heartbeat, sweeps sub-binds whose heartbeat has aged past the TTL, computes the connector aggregate
-- by strict majority of the LIVE sub-binds, stores it, and reports whether it still needs publishing.
--
-- KEYS[1] = breaker:binds:{connector_id}  (HASH: field "{pod_id}:{bind_index}" -> "state:heartbeat_ms")
-- KEYS[2] = breaker:state:{connector_id}  (STRING: derived aggregate token, read by the router)
-- KEYS[3] = breaker:acked:{connector_id}  (STRING: last aggregate that was successfully published)
-- ARGV[1] = field ("{pod_id}:{bind_index}")   ARGV[2] = this sub-bind's state token
-- ARGV[3] = now (ms)   ARGV[4] = sub-bind TTL (ms)   ARGV[5] = key TTL (ms)
-- Returns { needs_publish (0|1), aggregate_token }. needs_publish is 1 while the aggregate differs from
-- the last acknowledged (published) value, so a failed publish is retried on the next report.

local binds, statekey, ackedkey = KEYS[1], KEYS[2], KEYS[3]
local field, mystate = ARGV[1], ARGV[2]
local now, ttl, keyttl = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])

-- record this sub-bind's state + heartbeat.
redis.call('HSET', binds, field, mystate .. ':' .. now)

-- sweep expired (and any malformed) sub-binds; tally the live ones by state.
local all = redis.call('HGETALL', binds)
local nOpen, nHalf, nLive = 0, 0, 0
for i = 1, #all, 2 do
	local f, v = all[i], all[i + 1]
	local sep = string.find(v, ':', 1, true)
	local hb = nil
	if sep ~= nil then
		hb = tonumber(string.sub(v, sep + 1))
	end
	if hb == nil or (now - hb) > ttl then
		redis.call('HDEL', binds, f) -- silent, dead, or malformed sub-bind: excluded from the quorum
	else
		nLive = nLive + 1
		local st = string.sub(v, 1, sep - 1)
		if st == 'open' then
			nOpen = nOpen + 1
		elseif st == 'half_open' then
			nHalf = nHalf + 1
		end
	end
end

-- strict-majority aggregate; conservative default closed (an isolated pod must not fence everything).
local agg = 'closed'
if nLive > 0 then
	if nOpen * 2 > nLive then
		agg = 'open'
	elseif nHalf * 2 > nLive then
		agg = 'half_open'
	end
end

redis.call('SET', statekey, agg)
redis.call('PEXPIRE', binds, keyttl)
redis.call('PEXPIRE', statekey, keyttl)

-- needs_publish compares against the last acknowledged value (advanced by Go only after a successful
-- publish), so a lost/failed publish republishes next time rather than being dropped for good.
local acked = redis.call('GET', ackedkey)
if acked == false then
	acked = 'closed' -- an unset marker means nothing has been published yet; closed is the implicit baseline
end
redis.call('PEXPIRE', ackedkey, keyttl) -- keep it alive alongside the aggregate while reports flow
local needs = 0
if acked ~= agg then
	needs = 1
end
return { needs, agg }
