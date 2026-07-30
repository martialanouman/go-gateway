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

-- name: InsertLedgerEntry :exec
-- Append one ledger row. The ledger is APPEND-ONLY (§6.9): a row is never updated once written, so the
-- history is immutable and auditable. balance_after is supplied by the caller's atomic path.
INSERT INTO control_plane.billing_ledger
  (owner_type, owner_id, direction, customer_id, account_id, message_id, entry_type, credits,
   balance_after, reference)
VALUES
  (@owner_type, @owner_id, @direction, @customer_id, @account_id, @message_id, @entry_type, @credits,
   @balance_after, @reference);

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
