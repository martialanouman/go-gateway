ALTER TABLE cdr MODIFY TTL toDate(submitted_at) + INTERVAL 90 DAY SETTINGS materialize_ttl_after_modify = 0;
