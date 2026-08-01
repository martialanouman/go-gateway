-- name: GetBalance :one
-- The durable authority for one owner balance (owner_type, owner_id, direction). PostgreSQL owns the
-- balance; Redis caches it (§6.9). A missing row means zero credits ever recorded — the caller treats
-- absence as 0, not an error.
SELECT credits
FROM control_plane.balances
WHERE owner_type = @owner_type AND owner_id = @owner_id AND direction = @direction;

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

-- name: GetBalanceForUpdate :one
-- Read one owner balance and LOCK its row (§6.9, step-148 admin transfer / change-scope). The lock
-- serialises this admin tx against a concurrent durable mirror write to the same balance, so a transfer's
-- overdraw check and a change-scope zero-check see a stable value. A missing row is a legitimate 0.
SELECT credits FROM control_plane.balances
WHERE owner_type = @owner_type AND owner_id = @owner_id AND direction = @direction
FOR UPDATE;

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

-- name: LedgerEntryExists :one
-- The AUTHORITATIVE cross-partition idempotency guard (§6.9): whether a ledger entry of entry_type
-- already exists for message_id. Read before a capture so a redelivery of the same message_id never
-- double-charges. The unique index billing_ledger_idem_idx is only a same-day backstop and must not be
-- relied on alone at a day boundary — hence this explicit check, which spans every partition.
SELECT EXISTS (
  SELECT 1 FROM control_plane.billing_ledger
  WHERE message_id = @message_id AND entry_type = @entry_type
) AS entry_exists;

-- name: ClaimIdempotency :execrows
-- Claim (message_id, entry_type) in the partition-free idempotency table BEFORE applying a movement
-- (§6.9, invariant c). Returns the number of rows inserted: 1 = first time (proceed), 0 = the movement
-- already happened on some earlier attempt (a replay, possibly across a day boundary the ledger's
-- same-day index cannot see) — the caller skips the balance/ledger writes. The INSERT is the lock, so
-- two concurrent replays cannot both proceed (no read-then-write race).
INSERT INTO control_plane.billing_idempotency (message_id, entry_type)
VALUES (@message_id, @entry_type)
ON CONFLICT (message_id, entry_type) DO NOTHING;

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

-- name: AdjustBalance :one
-- Apply a SIGNED delta to the durable owner balance for a direction (credits += delta), creating the row
-- on first use, and RETURN the resulting balance. The delta form is order-independent: two concurrent
-- movements for the same owner commit in any order and the balance is always the sum of every delta —
-- which is exactly the append-only ledger's SUM(credits). An absolute set would let a stale write clobber
-- a fresher one under the concurrency this system runs at.
INSERT INTO control_plane.balances (owner_type, owner_id, direction, credits)
VALUES (@owner_type, @owner_id, @direction, @delta)
ON CONFLICT (owner_type, owner_id, direction)
DO UPDATE SET credits = control_plane.balances.credits + @delta, updated_at = now()
RETURNING credits;

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
