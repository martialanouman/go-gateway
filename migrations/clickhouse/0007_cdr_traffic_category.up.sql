-- Traffic category and effective priority of a message (ADR-0020, step-292). Written by every row after the
-- sender ID authorization, empty and 0 on the accepted placeholder and on rows that predate the columns.
ALTER TABLE cdr ADD COLUMN traffic_category LowCardinality(String) DEFAULT '', ADD COLUMN priority UInt8 DEFAULT 0
