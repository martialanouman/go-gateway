-- Durable lifecycle timeline (step-185). The cdr table cannot serve one: it is ReplacingMergeTree(version)
-- and `status` is not in its sorting key, so a background merge collapses a message's stages down to the
-- highest version. Reading pre-merge rows makes get-message-trace non-deterministic — the same message
-- answers with four stages, then two, at a moment the merge scheduler decides.
--
-- Append-only MergeTree: nothing is ever merged away. ORDER BY starts with message_id so a trace lookup is
-- a primary-key prefix scan, not a full-table scan.
CREATE TABLE IF NOT EXISTS cdr_events
(
    message_id    UUID,
    at            DateTime64(3),
    segment_seq   UInt16,
    status        Enum8('accepted'=1,'enroute'=2,'delivered'=3,'failed'=4,'expired'=5,'rejected'=6,'rerouted'=7,'cancelled'=8),
    version       UInt64,
    error_code    Nullable(String),
    connector_id  Nullable(UUID),
    latency_ms    Nullable(UInt32)
)
ENGINE = MergeTree
PARTITION BY toDate(at)
ORDER BY (message_id, at, segment_seq, version)
TTL toDate(at) + INTERVAL 90 DAY
