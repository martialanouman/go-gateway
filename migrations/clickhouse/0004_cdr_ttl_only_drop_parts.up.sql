-- Make the table's own row TTL drop WHOLE PARTS instead of rewriting them row by row (§14, step-165).
-- With the default (ttl_only_drop_parts = 0) an expiring row TTL rewrites each part to remove the expired
-- rows — a continuous background rewrite of the CDR at 8000 msg/s, exactly the delete-by-predicate cost the
-- retention design exists to avoid. With 1, a part is dropped only once ALL of its rows have expired, which
-- for daily partitions means the partition disappears as a metadata operation.
ALTER TABLE cdr MODIFY SETTING ttl_only_drop_parts = 1;
