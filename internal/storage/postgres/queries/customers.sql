-- name: CreateCustomer :one
-- Optional columns fall back to their DDL defaults via COALESCE, so the schema stays the one
-- authority on what a defaulted value is.
INSERT INTO control_plane.customers (
    name, group_id, rate_plan_id, billing_enabled, billing_mode,
    overdraft_enabled, overdraft_limit, balance_scope, mo_billing_floor,
    content_storage, content_retention_days
) VALUES (
    @name,
    sqlc.narg('group_id'),
    sqlc.narg('rate_plan_id'),
    @billing_enabled,
    sqlc.narg('billing_mode'),
    @overdraft_enabled,
    sqlc.narg('overdraft_limit'),
    COALESCE(sqlc.narg('balance_scope')::text, 'customer'),
    sqlc.narg('mo_billing_floor'),
    COALESCE(sqlc.narg('content_storage')::text, 'inherit'),
    sqlc.narg('content_retention_days')
)
RETURNING *;

-- name: GetCustomer :one
SELECT * FROM control_plane.customers WHERE id = @id;

-- name: ListCustomers :many
-- Keyset pagination on the UUIDv7 primary key (time-ordered). The caller asks for limit+1 rows to
-- learn whether a further page exists.
SELECT * FROM control_plane.customers
WHERE (sqlc.narg('group_id')::uuid IS NULL OR group_id = sqlc.narg('group_id'))
  AND (sqlc.narg('status')::text  IS NULL OR status   = sqlc.narg('status'))
  AND (sqlc.narg('after')::uuid   IS NULL OR id       > sqlc.narg('after'))
ORDER BY id
LIMIT @lim;

-- name: UpdateCustomer :one
-- Partial update: a NULL argument leaves its column unchanged (COALESCE). group_id is not here —
-- group membership has its own endpoint.
UPDATE control_plane.customers SET
    name                   = COALESCE(sqlc.narg('name'), name),
    status                 = COALESCE(sqlc.narg('status'), status),
    rate_plan_id           = COALESCE(sqlc.narg('rate_plan_id'), rate_plan_id),
    billing_enabled        = COALESCE(sqlc.narg('billing_enabled'), billing_enabled),
    billing_mode           = COALESCE(sqlc.narg('billing_mode'), billing_mode),
    overdraft_enabled      = COALESCE(sqlc.narg('overdraft_enabled'), overdraft_enabled),
    overdraft_limit        = COALESCE(sqlc.narg('overdraft_limit'), overdraft_limit),
    mo_billing_floor       = COALESCE(sqlc.narg('mo_billing_floor'), mo_billing_floor),
    content_storage        = COALESCE(sqlc.narg('content_storage'), content_storage),
    content_retention_days = COALESCE(sqlc.narg('content_retention_days'), content_retention_days)
WHERE id = @id
RETURNING *;

-- name: DeleteCustomer :execrows
DELETE FROM control_plane.customers WHERE id = @id;

-- name: SuspendCustomer :one
UPDATE control_plane.customers SET status = 'suspended' WHERE id = @id RETURNING *;

-- name: SuspendCustomerAccounts :exec
UPDATE control_plane.smpp_accounts SET status = 'suspended' WHERE customer_id = @customer_id;
