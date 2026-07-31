-- The Retainer (step-165) owns CDR retention: it archives an expired daily partition to cold storage and
-- then DROPs it. The table's own row TTL must therefore NOT delete the same rows first — at the shipped
-- default (both 90 days) ClickHouse removed a partition's rows a day before the Retainer considered the
-- partition expired, so tiering archived an EMPTY partition and then dropped it. Silent data loss.
--
-- The table TTL becomes a far backstop instead: it only fires if the Retainer has been disabled or stuck for
-- a year, and (with ttl_only_drop_parts from 0004) it drops whole parts rather than rewriting them.
-- materialize_ttl_after_modify = 0 keeps the change from scheduling a full-table rewrite.
ALTER TABLE cdr MODIFY TTL toDate(submitted_at) + INTERVAL 400 DAY SETTINGS materialize_ttl_after_modify = 0;
