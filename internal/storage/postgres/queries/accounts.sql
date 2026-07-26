-- name: CreateAccount :one
INSERT INTO control_plane.smpp_accounts (
    customer_id, name, smpp_enabled, rest_enabled, sender_id_policy,
    query_sm_enabled, cancel_sm_enabled, allowed_bind_types, max_sessions
) VALUES (
    @customer_id,
    @name,
    COALESCE(sqlc.narg('smpp_enabled')::boolean, true),
    COALESCE(sqlc.narg('rest_enabled')::boolean, true),
    COALESCE(sqlc.narg('sender_id_policy')::text, 'strict'),
    COALESCE(sqlc.narg('query_sm_enabled')::boolean, true),
    COALESCE(sqlc.narg('cancel_sm_enabled')::boolean, true),
    COALESCE(sqlc.narg('allowed_bind_types')::text, 'trx'),
    COALESCE(sqlc.narg('max_sessions')::integer, 1)
)
RETURNING *;

-- name: GetAccount :one
SELECT * FROM control_plane.smpp_accounts WHERE id = @id;

-- name: ListAccounts :many
SELECT a.* FROM control_plane.smpp_accounts a
WHERE (sqlc.narg('customer_id')::uuid IS NULL OR a.customer_id = sqlc.narg('customer_id'))
  AND (sqlc.narg('status')::text     IS NULL OR a.status      = sqlc.narg('status'))
  AND (sqlc.narg('after')::uuid      IS NULL OR a.id          > sqlc.narg('after'))
  AND (
        sqlc.narg('group_id')::uuid IS NULL
        OR a.customer_id IN (
            SELECT c.id FROM control_plane.customers c WHERE c.group_id = sqlc.narg('group_id')
        )
      )
ORDER BY a.id
LIMIT @lim;

-- name: UpdateAccount :one
UPDATE control_plane.smpp_accounts SET
    name   = COALESCE(sqlc.narg('name'), name),
    status = COALESCE(sqlc.narg('status'), status)
WHERE id = @id
RETURNING *;

-- name: DeleteAccount :execrows
DELETE FROM control_plane.smpp_accounts WHERE id = @id;

-- name: SetAccountChannels :one
UPDATE control_plane.smpp_accounts SET smpp_enabled = @smpp_enabled, rest_enabled = @rest_enabled
WHERE id = @id
RETURNING *;

-- name: SetAccountSessionLimits :one
UPDATE control_plane.smpp_accounts SET max_sessions = @max_sessions, allowed_bind_types = @allowed_bind_types
WHERE id = @id
RETURNING *;

-- name: SuspendAccount :one
UPDATE control_plane.smpp_accounts SET status = 'suspended' WHERE id = @id RETURNING *;

-- name: ListAccountCustomers :many
-- Lightweight account -> customer projection for the MO router's snapshot (step-045): resolving an
-- inbound number or keyword to an account needs the owning customer for the routed envelope.
SELECT id, customer_id FROM control_plane.smpp_accounts;

-- name: ListAccountSenderIDPolicies :many
-- account -> (customer, sender_id_policy) projection for the sender-ID authorization snapshot
-- (step-060). The policy is per account; the registered sender IDs it is checked against are per
-- customer.
SELECT id, customer_id, sender_id_policy FROM control_plane.smpp_accounts;
