-- name: GetBalance :one
-- The durable authority for one owner balance (owner_type, owner_id, direction). PostgreSQL owns the
-- balance; Redis caches it (§6.9). A missing row means zero credits ever recorded — the caller treats
-- absence as 0, not an error.
SELECT credits
FROM control_plane.balances
WHERE owner_type = @owner_type AND owner_id = @owner_id AND direction = @direction;

-- name: GetBillingCustomer :one
-- A customer's MT billing configuration. A missing row means billing is not configured for the customer
-- — the caller treats absence as "unconfigured", not an error.
SELECT billing_mode, overdraft_enabled, overdraft_limit, credit_limit, credit_limit_is_hard,
       external_billing_provider_id
FROM control_plane.billing_customers
WHERE customer_id = @customer_id;

-- name: LedgerEntryExists :one
-- The AUTHORITATIVE cross-partition idempotency guard (§6.9): whether a ledger entry of entry_type
-- already exists for message_id. Read before a capture so a redelivery of the same message_id never
-- double-charges. The unique index billing_ledger_idem_idx is only a same-day backstop and must not be
-- relied on alone at a day boundary — hence this explicit check, which spans every partition.
SELECT EXISTS (
  SELECT 1 FROM control_plane.billing_ledger
  WHERE message_id = @message_id AND entry_type = @entry_type
) AS entry_exists;

-- name: InsertLedgerEntry :exec
-- Append one ledger row. The ledger is APPEND-ONLY (§6.9): a row is never updated once written, so the
-- history is immutable and auditable. balance_after is supplied by the caller's atomic path.
INSERT INTO control_plane.billing_ledger
  (owner_type, owner_id, direction, customer_id, account_id, message_id, entry_type, credits,
   balance_after, reference)
VALUES
  (@owner_type, @owner_id, @direction, @customer_id, @account_id, @message_id, @entry_type, @credits,
   @balance_after, @reference);

-- name: UpsertBalance :exec
-- Set the durable owner balance for a direction to `credits`, creating the row on first use. The balance
-- table is the authority Redis caches; the caller passes the balance its atomic path computed, so this
-- write reconciles the durable authority with the cached hot-path value rather than computing anything.
INSERT INTO control_plane.balances (owner_type, owner_id, direction, credits)
VALUES (@owner_type, @owner_id, @direction, @credits)
ON CONFLICT (owner_type, owner_id, direction)
DO UPDATE SET credits = EXCLUDED.credits, updated_at = now();
