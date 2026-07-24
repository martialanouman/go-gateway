-- finalize.lua — flip a reserved entry to "done" once its message is durably published.
--
-- KEYS[1] = idem:{account_id}:<key>
--
-- Only an existing entry is touched: if the 24 h window already lapsed the entry is gone and a fresh
-- submit will re-reserve, so a bare HSET (which would recreate a TTL-less orphan) is avoided.
-- Returns 1 when the entry was present and flipped, 0 when it had lapsed.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], 'state', 'done')
return 1
