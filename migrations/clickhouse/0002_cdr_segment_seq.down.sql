-- Reverse step-082c's per-segment CDR key: narrow the sorting key back to a prefix (which existing
-- parts already satisfy) and drop the column. Both actions share one ALTER so segment_seq leaves the
-- key before it is dropped.
ALTER TABLE cdr MODIFY ORDER BY (customer_id, account_id, submitted_at, message_id), DROP COLUMN segment_seq;
