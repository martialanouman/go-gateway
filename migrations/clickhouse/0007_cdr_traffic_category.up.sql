-- Traffic category and effective priority of a message (ADR-0020, step-292). Written by every segment row,
-- empty and 0 on the message-level rows and on rows that predate the columns.
ALTER TABLE cdr ADD COLUMN traffic_category LowCardinality(String) DEFAULT '' AFTER credits_charged, ADD COLUMN priority UInt8 DEFAULT 0 AFTER traffic_category
