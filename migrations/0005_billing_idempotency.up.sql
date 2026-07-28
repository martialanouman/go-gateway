-- Cross-partition idempotency for the billing lifecycle (§6.9, invariant c). The billing_ledger's
-- idem index includes the partition key (created_at) and so can only dedup within a single day
-- partition. A message replayed AFTER its short-TTL Redis hold has lapsed AND across a day boundary
-- (original just before midnight, replay after) escapes that index: the ledger insert lands in a
-- different partition, so a reserve would DOUBLE-DEBIT. This table is the authoritative, partition-free
-- guard: RecordDurable claims (message_id, entry_type) here FIRST, in the same transaction; a duplicate
-- INSERT conflicts (0 rows) and the movement is skipped — the INSERT itself is the lock, so there is no
-- read-then-write TOCTOU. It is NOT partitioned (partitioning would reintroduce the gap at a wider
-- boundary). entry_type is restricted to the message-bearing lifecycle types (top-ups/adjustments have
-- no message_id and are never claimed here).
CREATE TABLE control_plane.billing_idempotency (
  message_id uuid NOT NULL,
  entry_type text NOT NULL CHECK (entry_type IN ('reserve','capture','release','refund')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (message_id, entry_type)
);

-- created_at supports the retention purge: a message can no longer be redelivered once it falls out of
-- the Kafka retention window, so rows older than (retention + margin) are safe to DELETE, keeping this
-- table small in steady state (a scheduled job, §6.14).
CREATE INDEX billing_idempotency_created_idx ON control_plane.billing_idempotency(created_at);
