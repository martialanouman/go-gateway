-- Ingress channel of an MT, http or smpp (step-403). Written on the message-level rows only; '' on segment
-- rows, on MO and on rows that predate the column.
ALTER TABLE cdr ADD COLUMN ingress_channel LowCardinality(String) DEFAULT '' AFTER priority
