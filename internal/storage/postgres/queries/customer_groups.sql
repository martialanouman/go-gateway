-- name: CreateCustomerGroup :one
-- status falls back to the DDL default ('active'). created_by is the BFF's operator id (ADR-0019), NULL
-- under a static token.
INSERT INTO control_plane.customer_groups AS g (name, description, created_by)
VALUES (@name, sqlc.narg('description'), sqlc.narg('created_by'))
RETURNING sqlc.embed(g), (SELECT count(*) FROM control_plane.customers c WHERE c.group_id = g.id)::bigint AS member_count;

-- name: GetCustomerGroup :one
SELECT sqlc.embed(g), (SELECT count(*) FROM control_plane.customers c WHERE c.group_id = g.id)::bigint AS member_count
FROM control_plane.customer_groups g WHERE g.id = @id;

-- name: ListCustomerGroups :many
-- Not paginated: the contract returns a bare array. A group count is an operator-scale number —
-- one row per commercial segment — not a subscriber-scale one, so there is no keyset here.
SELECT sqlc.embed(g), (SELECT count(*) FROM control_plane.customers c WHERE c.group_id = g.id)::bigint AS member_count
FROM control_plane.customer_groups g
WHERE (sqlc.narg('status')::text IS NULL OR g.status = sqlc.narg('status'))
ORDER BY g.name;

-- name: UpdateCustomerGroup :one
-- Partial update: a NULL argument leaves its column unchanged (COALESCE). updated_at is not set
-- here — the customer_groups_touch trigger does it. Consequence of COALESCE: description cannot be
-- cleared back to NULL through this path, although the contract marks it nullable
-- (debts/patch-null-ne-peut-pas-effacer-un-champ.md).
UPDATE control_plane.customer_groups AS g SET
    name        = COALESCE(sqlc.narg('name'), name),
    description = COALESCE(sqlc.narg('description'), description),
    status      = COALESCE(sqlc.narg('status'), status)
WHERE g.id = @id
RETURNING sqlc.embed(g), (SELECT count(*) FROM control_plane.customers c WHERE c.group_id = g.id)::bigint AS member_count;

-- name: DeleteCustomerGroup :execrows
-- Non-destructive by schema (§6.17): customers.group_id references this table ON DELETE SET NULL,
-- so deleting a group detaches its customers and deletes none of them. There is deliberately no
-- application-side cascade to contradict it.
DELETE FROM control_plane.customer_groups WHERE id = @id;
