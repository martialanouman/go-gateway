-- name: GetBalance :one
-- The durable owner balance (owner_type, owner_id, direction) is the folded balances row PLUS the owner's
-- unfolded deltas (ADR-0022), read in ONE statement so a concurrent fold cannot hide a delta from both halves
-- or show it in both. found=false means nothing was ever recorded for the owner — a legitimate zero.
SELECT (COALESCE(f.credits, 0) + COALESCE(p.total, 0))::int AS credits,
       (f.credits IS NOT NULL OR p.total IS NOT NULL) AS found
FROM (SELECT (SELECT b.credits FROM control_plane.balances b
               WHERE b.owner_type = @owner_type AND b.owner_id = @owner_id AND b.direction = @direction) AS credits) f,
     (SELECT sum(d.credits) AS total FROM control_plane.balance_deltas d
       WHERE d.owner_type = @owner_type AND d.owner_id = @owner_id AND d.direction = @direction) p;

-- name: GetBillingCustomer :one
-- A customer's MT billing configuration. Billing config lives on the customer row itself (§6.9, step-142d
-- consolidation); a missing customer row is not_found. billing_mode NULL means unset (treated as strict
-- prepaid by the floor mapping).
SELECT billing_mode, overdraft_enabled, overdraft_limit, credit_limit, credit_limit_is_hard,
       mo_billing_floor, external_billing_provider_id
FROM control_plane.customers
WHERE id = @customer_id;

-- name: ListBillingCustomers :many
-- The MT billing configuration of every billing-enabled customer, for config-sync to compile the
-- reserve-floor snapshot (step-142b/d). Read whole and swapped atomically; a customer absent from the
-- result (billing disabled, or not yet created) fails closed to strict prepaid at read time.
SELECT id AS customer_id, billing_mode, overdraft_enabled, overdraft_limit, credit_limit,
       credit_limit_is_hard, mo_billing_floor, external_billing_provider_id
FROM control_plane.customers
WHERE billing_enabled;

-- name: ListBillingScopes :many
-- The identity and balance scope of every billing-enabled customer, for router-svc to compile the
-- credit-stage gate + owner-resolution snapshot (step-145). Presence in the result IS the billing-enabled
-- flag: a customer absent from it (billing disabled, or not yet created) is not billed and makes NO billing
-- round-trip. balance_scope NULL defaults to 'customer' at read time (the schema default).
SELECT id AS customer_id, balance_scope
FROM control_plane.customers
WHERE billing_enabled;

-- name: ListExternalBillingConfigs :many
-- The ACTIVE external billing provider (§6.10) of every billing-enabled customer that references one — the
-- customers→providers join, read together with ListBillingCustomers so billing-svc compiles a consistent
-- snapshot. A customer with no provider, or whose provider is disabled, is absent (external layer off). The
-- inner join guarantees no dangling reference; status='active' is the operator kill switch.
SELECT c.id AS customer_id, p.id AS provider_id, p.mode, p.sync_call_timeout_ms,
       p.failure_policy, p.cache_ttl_ms
FROM control_plane.customers c
JOIN control_plane.external_billing_providers p ON c.external_billing_provider_id = p.id
WHERE c.billing_enabled AND p.status = 'active';

-- name: ConsumedCredits :one
-- The customer's LOCALLY SETTLED MT consumption (§6.10 reconciliation): the sum of reserve debits for
-- messages that were captured (not released). Reserve debits are negative, so negate to a positive consumed
-- total; a message reserved-then-released nets nothing and is excluded by the capture EXISTS. In-flight holds
-- (reserved, not yet captured) are legitimate skew and are excluded. Compared off the critical path against
-- the external provider's reported usage; a difference is reported, never auto-corrected.
SELECT COALESCE(-SUM(l.credits), 0)::bigint AS consumed
FROM control_plane.billing_ledger l
WHERE l.customer_id = @customer_id AND l.direction = 'mt' AND l.entry_type = 'reserve'
  AND EXISTS (
    SELECT 1 FROM control_plane.billing_ledger c
    WHERE c.message_id = l.message_id AND c.entry_type = 'capture'
      AND c.customer_id = l.customer_id AND c.direction = 'mt'
  );

-- name: LockBalance :exec
-- Lock one owner's balances row (§6.9, step-148 admin transfer / change-scope) against another admin tx and
-- the fold; the hot path appends deltas without it, and Redis is its serialisation point. Read the balance
-- in a LATER statement: under READ COMMITTED a statement that waits on this lock re-reads the row's newest
-- version but keeps its older snapshot of balance_deltas, and would count a just-folded delta twice.
-- An absent row is created and locked: otherwise the fold could insert it mid-transaction and deadlock.
INSERT INTO control_plane.balances (owner_type, owner_id, direction, credits)
VALUES (@owner_type, @owner_id, @direction, 0)
ON CONFLICT (owner_type, owner_id, direction) DO UPDATE SET credits = control_plane.balances.credits;

-- name: LockCustomerScope :one
-- Read a customer's current balance_scope and LOCK the customer row, so two concurrent change-scope admin
-- txns on the same customer serialise (step-148). not_found if the customer does not exist.
SELECT balance_scope FROM control_plane.customers WHERE id = @id FOR UPDATE;

-- name: UpdateBalanceScope :execrows
-- Flip a customer's balance_scope (step-148). Returns rows affected: 0 = no such customer. The same-table
-- CHECK (step-142c) still rejects 'smpp_account' when a hard credit limit or overdraft is set — that raises
-- a CHECK violation the caller's translate() maps to validation_error (422), not a 500.
UPDATE control_plane.customers
SET balance_scope = @balance_scope, updated_at = now()
WHERE id = @id;

-- name: ListLedger :many
-- One keyset page of a customer's billing ledger, newest first (step-149 get-billing-ledger). Optional
-- direction/account_id filters. The keyset is (created_at, id) DESC so rows sharing a created_at are not
-- dropped (id is a time-ordered UUIDv7). A NULL after_created is the first page. Never selects a message body
-- (the ledger has none). @lim is fetched as page+1 so the caller can detect a further page.
SELECT id, owner_type, owner_id, direction, customer_id, account_id, message_id, entry_type, credits,
       balance_after, reference, created_at
FROM control_plane.billing_ledger
WHERE customer_id = @customer_id
  AND (sqlc.narg('direction')::text IS NULL OR direction = sqlc.narg('direction'))
  AND (sqlc.narg('account_id')::uuid IS NULL OR account_id = sqlc.narg('account_id'))
  AND (sqlc.narg('after_created')::timestamptz IS NULL
       OR created_at < sqlc.narg('after_created')
       OR (created_at = sqlc.narg('after_created') AND id < sqlc.narg('after_id')))
ORDER BY created_at DESC, id DESC
LIMIT @lim;

-- name: GetMessageEntries :one
-- Which MT lifecycle entries exist for message_id, in ONE read across every partition (step-287k): the
-- capture and release paths decide on it under their terminal lock. Replaces two entry-existence reads and a
-- reserve read; spans every partition, since the ledger's own unique index cannot (§6.9).
SELECT
  COALESCE(bool_or(entry_type = 'reserve'), false)::bool AS has_reserve,
  COALESCE(bool_or(entry_type = 'capture'), false)::bool AS has_capture,
  COALESCE(bool_or(entry_type = 'release'), false)::bool AS has_release
FROM control_plane.billing_ledger
WHERE message_id = @message_id AND entry_type IN ('reserve', 'capture', 'release');

-- name: ClaimIdempotency :execrows
-- Claim (message_id, entry_type) in the partition-free idempotency table BEFORE applying a movement
-- (§6.9, invariant c). Returns the number of rows inserted: 1 = first time (proceed), 0 = the movement
-- already happened on some earlier attempt (a replay, possibly across a day boundary the ledger's
-- same-day index cannot see) — the caller skips the balance/ledger writes. The INSERT is the lock, so
-- two concurrent replays cannot both proceed (no read-then-write race).
INSERT INTO control_plane.billing_idempotency (message_id, entry_type)
VALUES (@message_id, @entry_type)
ON CONFLICT (message_id, entry_type) DO NOTHING;

-- name: XactStatus :one
-- committed, aborted or in progress; NULL once the id is too old for the commit log to remember.
SELECT pg_xact_status(CAST(CAST(sqlc.arg(xid) AS text) AS xid8))::text AS status;

-- name: WriteBillingBatch :one
-- RecordDurable for a whole batch in one statement (step-287f): every round trip pays the network to Postgres.
-- The claim returns the (message_id, entry_type) pairs it inserted, and only those reach the deltas and the
-- ledger; claimed lists their positions in the batch. A pair twice in the batch is claimed once but joins
-- both copies, and the ledger's unique index then refuses the second, as one transaction has one now().
-- Claimed in key order: two concurrent batches holding the same pairs in opposite orders would otherwise
-- deadlock. The transaction id comes back with the write, before the COMMIT that may go unanswered.
WITH input AS (
  SELECT (e->>'ord')::int AS ord, e->>'owner_type' AS owner_type, (e->>'owner_id')::uuid AS owner_id,
    e->>'direction' AS direction, (e->>'customer_id')::uuid AS customer_id,
    (e->>'account_id')::uuid AS account_id, (e->>'message_id')::uuid AS message_id,
    e->>'entry_type' AS entry_type, (e->>'credits')::int AS credits,
    (e->>'balance_after')::int AS balance_after, e->>'reference' AS reference
  FROM jsonb_array_elements(@entries::jsonb) AS e
), claimed AS (
  INSERT INTO control_plane.billing_idempotency (message_id, entry_type)
  SELECT input.message_id, input.entry_type FROM input ORDER BY input.message_id, input.entry_type
  ON CONFLICT (message_id, entry_type) DO NOTHING
  RETURNING message_id, entry_type
), fresh AS (
  SELECT input.ord, input.owner_type, input.owner_id, input.direction, input.customer_id, input.account_id,
    input.message_id, input.entry_type, input.credits, input.balance_after, input.reference
  FROM input JOIN claimed USING (message_id, entry_type)
), deltas AS (
  INSERT INTO control_plane.balance_deltas (owner_type, owner_id, direction, credits)
  SELECT fresh.owner_type, fresh.owner_id, fresh.direction, fresh.credits FROM fresh WHERE fresh.credits <> 0
), ledger AS (
  INSERT INTO control_plane.billing_ledger
    (owner_type, owner_id, direction, customer_id, account_id, message_id, entry_type, credits,
     balance_after, reference)
  SELECT fresh.owner_type, fresh.owner_id, fresh.direction, fresh.customer_id, fresh.account_id,
    fresh.message_id, fresh.entry_type, fresh.credits, fresh.balance_after, fresh.reference
  FROM fresh
)
SELECT pg_current_xact_id()::text AS xid, ARRAY(SELECT fresh.ord FROM fresh)::int[] AS claimed;

-- name: InsertLedgerEntry :one
-- Append one ledger row and return its generated id and created_at. The ledger is APPEND-ONLY (§6.9): a row
-- is never updated once written, so the history is immutable and auditable. balance_after is supplied by the
-- caller's atomic path. The returned id/created_at let an admin top-up/transfer echo the created entries.
INSERT INTO control_plane.billing_ledger
  (owner_type, owner_id, direction, customer_id, account_id, message_id, entry_type, credits,
   balance_after, reference)
VALUES
  (@owner_type, @owner_id, @direction, @customer_id, @account_id, @message_id, @entry_type, @credits,
   @balance_after, @reference)
RETURNING id, created_at;

-- name: AdjustBalance :exec
-- Apply a SIGNED delta to the durable owner balance for a direction (credits += delta), creating the row
-- on first use. The delta form is order-independent: two concurrent movements for the same owner commit in
-- any order and the balance is always the sum of every delta; with the unfolded deltas (ADR-0022), that is
-- the append-only ledger's SUM(credits). An absolute set would let a stale write clobber
-- a fresher one under the concurrency this system runs at.
INSERT INTO control_plane.balances (owner_type, owner_id, direction, credits)
VALUES (@owner_type, @owner_id, @direction, @delta)
ON CONFLICT (owner_type, owner_id, direction)
DO UPDATE SET credits = control_plane.balances.credits + @delta, updated_at = now();

-- name: GetReserveEntry :one
-- The reserve ledger entry for a message_id (the amount of record, §6.9). The capture and release paths
-- read it to recover the reserved amount and the post-reserve balance when the short-TTL Redis hold has
-- lapsed. credits is the signed reserve delta (negative); balance_after is the balance right after the
-- reserve. The latest reserve wins (there is at most one per message under normal operation).
SELECT credits, balance_after
FROM control_plane.billing_ledger
WHERE message_id = @message_id AND entry_type = 'reserve'
ORDER BY created_at DESC
LIMIT 1;

-- name: ListOrphanedReservations :many
-- The reaper's detection query (step-190): reservations whose money was never closed. A message is
-- orphaned when it holds a `reserve` claim but NEITHER a `capture` NOR a `release` — the settle loop in
-- connector-pool fails open, so a billing outage leaves the reserve debit standing with nothing to
-- reconcile it.
--
-- Detection reads control_plane.billing_idempotency, NOT the ledger's own idempotency index: that index
-- must include the partition key (created_at) and therefore cannot span day partitions, so a reservation
-- settled across midnight would look unsettled to it. billing_idempotency is unpartitioned and
-- authoritative, and its created_at index keeps this sweep cheap.
--
-- The LATERAL recovers the owner and amount from the ledger, which billing_idempotency does not carry.
-- It is a CROSS JOIN on purpose: a claim whose ledger partition has already been detached to object
-- storage (§6.14.2) drops out of the result, because a reservation whose ledger row is archived can no
-- longer be settled and must not be reported as actionable.
SELECT i.message_id, r.owner_type, r.owner_id, r.customer_id, r.account_id, r.credits,
       i.created_at AS reserved_at
FROM control_plane.billing_idempotency i
CROSS JOIN LATERAL (
  SELECT owner_type, owner_id, customer_id, account_id, credits
  FROM control_plane.billing_ledger
  WHERE message_id = i.message_id AND entry_type = 'reserve'
  ORDER BY created_at DESC
  LIMIT 1
) r
WHERE i.entry_type = 'reserve'
  AND i.created_at < @older_than
  AND NOT EXISTS (
    SELECT 1 FROM control_plane.billing_idempotency s
    WHERE s.message_id = i.message_id AND s.entry_type IN ('capture', 'release')
  )
ORDER BY i.created_at
LIMIT @row_limit;

-- name: InsertBalanceDelta :exec
-- Record a hot-path balance movement without touching the owner's balances row, whose lock serialised every
-- reserve of a customer (ADR-0022). The fold moves it into balances later.
INSERT INTO control_plane.balance_deltas (owner_type, owner_id, direction, credits)
VALUES (@owner_type, @owner_id, @direction, @credits);

-- name: FoldBalanceDeltas :one
-- Move up to @lim pending deltas into balances in ONE statement, so a reader sees each delta either pending
-- or folded, never both nor neither. SKIP LOCKED splits the work between replicas; the owner ORDER BY keeps
-- their row locks in one order. Returns the number of deltas folded (a full batch means more are waiting).
WITH moved AS (
  DELETE FROM control_plane.balance_deltas
  WHERE id IN (
    SELECT id FROM control_plane.balance_deltas ORDER BY id LIMIT @lim FOR UPDATE SKIP LOCKED
  )
  RETURNING owner_type, owner_id, direction, credits
), upserted AS (
  INSERT INTO control_plane.balances (owner_type, owner_id, direction, credits)
  SELECT owner_type, owner_id, direction, sum(credits)::int FROM moved
  GROUP BY owner_type, owner_id, direction
  ORDER BY owner_type, owner_id, direction
  ON CONFLICT (owner_type, owner_id, direction)
  DO UPDATE SET credits = control_plane.balances.credits + excluded.credits, updated_at = now()
  RETURNING 1
)
SELECT count(*) FROM moved;

-- name: OldestBalanceDelta :one
SELECT created_at FROM control_plane.balance_deltas ORDER BY id LIMIT 1;

-- name: InsertBillingEvent :exec
-- Queue an MO floor crossing for billing.events (step-400), in the transaction of the mo_charge that crossed.
INSERT INTO control_plane.billing_events_outbox (owner_type, owner_id, customer_id, balance_after, floor)
VALUES (@owner_type, @owner_id, @customer_id, @balance_after, @floor);

-- name: ListPendingBillingEvents :many
SELECT id, owner_type, owner_id, customer_id, balance_after, floor, created_at
FROM control_plane.billing_events_outbox
ORDER BY id
LIMIT @lim;

-- name: DeleteBillingEvents :exec
DELETE FROM control_plane.billing_events_outbox WHERE id = ANY(@ids::uuid[]);

-- name: OldestBillingEvent :one
SELECT created_at FROM control_plane.billing_events_outbox ORDER BY id LIMIT 1;
