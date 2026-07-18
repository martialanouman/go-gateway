-- CDR / analytics store (ClickHouse dialect, specification §3.4) — versioned write model (§1.10).
--
-- Per-message mutation is infeasible at 8000 msg/s, so a status change is never an UPDATE: it is a
-- NEW row carrying the same message_id and a higher `version`. ReplacingMergeTree keeps the row
-- with the highest version per sorting key during background merges, and a read takes the latest
-- version explicitly (argMax / FINAL) so it is correct even before a merge has run.
--
-- `version` is a LIFECYCLE RANK, not a wall-clock timestamp. The later lifecycle stage must always
-- supersede regardless of which service wrote the row or of clock skew between hosts (the
-- `accepted` row is written by rest-api-svc, `enroute` by connector-pool-svc). Ranks, spaced to
-- leave room for M4+ states:
--     accepted=10  enroute=20  rejected=20  rerouted=30  delivered=40  failed=50  expired=50  cancelled=60
-- In M2 a message is `accepted` then exactly one of enroute/rejected/failed, so ranks never tie.
-- (M4 may widen this to a composite `rank<<k | attempt` when DLRs and retries revisit states.)
--
-- `submitted_at` is IMMUTABLE and repeated on every row for a message, so PARTITION BY
-- toDate(submitted_at) keeps all of a message's status rows in one partition even when a later
-- status arrives on another day.
--
-- The status enum is the full REST MessageStatus lifecycle (api/openapi-public.yaml): it adds
-- `accepted` (the pre-dispatch row that makes GET /messages/{id} 404-free, §1.10) and `cancelled`
-- (M3) to the six the connector/DLR path writes.

CREATE TABLE IF NOT EXISTS cdr
(
    message_id            UUID,
    trace_id              UUID,
    account_id            UUID,
    customer_id           UUID,
    direction             Enum8('mt' = 1, 'mo' = 2),
    source_addr           String,
    dest_addr             String,
    original_source_addr  Nullable(String),
    connector_id          Nullable(UUID),
    route_id              Nullable(UUID),
    routing_script_id     Nullable(UUID),
    submitted_at          DateTime64(3),
    delivered_at          Nullable(DateTime64(3)),
    status                Enum8('accepted'=1,'enroute'=2,'delivered'=3,'failed'=4,'expired'=5,'rejected'=6,'rerouted'=7,'cancelled'=8),
    error_code            Nullable(String),
    segment_count         UInt16,
    encoding              Enum8('gsm7'=1,'ucs2'=2,'binary'=3),
    content_ciphertext    Nullable(String),   -- present only when content_storage is stored_* ; NEVER in logs
    content_key_id        Nullable(UUID),
    latency_ms            Nullable(UInt32),
    billed                UInt8,               -- 0/1
    credits_charged       Nullable(Int32),
    version               UInt64               -- lifecycle rank; ReplacingMergeTree keeps the max
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(submitted_at)               -- daily partitions, TTL tiering (§6.14)
ORDER BY (customer_id, account_id, submitted_at, message_id)  -- all four immutable per message
TTL toDate(submitted_at) + INTERVAL 90 DAY;     -- CDR retention (configurable); body has its own shorter TTL
