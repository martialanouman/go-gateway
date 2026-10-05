-- name: CreateSenderID :one
-- The customer_id comes from the path; an unknown one violates the FK -> 422. A duplicate address
-- for the customer violates sender_ids_uq -> 409.
INSERT INTO control_plane.sender_ids (customer_id, address, traffic_category, created_by)
VALUES (@customer_id, @address, COALESCE(sqlc.narg('traffic_category')::text, 'marketing'), sqlc.narg('created_by'))
RETURNING *;

-- name: ListSenderIDsByCustomer :many
SELECT * FROM control_plane.sender_ids
WHERE customer_id = @customer_id
  AND (sqlc.narg('traffic_category')::text IS NULL OR traffic_category = sqlc.narg('traffic_category'))
ORDER BY address;

-- name: ListActiveSenderIDs :many
-- All active sender IDs across customers, for the sender-ID authorization snapshot (step-060). Only
-- 'active' rows count: a pending or disabled registration must not authorize a source address.
SELECT * FROM control_plane.sender_ids WHERE status = 'active';

-- name: UpdateSenderID :one
UPDATE control_plane.sender_ids SET
    status           = COALESCE(sqlc.narg('status'), status),
    traffic_category = COALESCE(sqlc.narg('traffic_category'), traffic_category)
WHERE customer_id = @customer_id AND id = @id
RETURNING *;

-- name: DeleteSenderID :execrows
-- A sender ID that has sent is never deleted (ADR-0023): zero rows is then a conflict, not a miss.
DELETE FROM control_plane.sender_ids
WHERE customer_id = @customer_id AND id = @id AND first_used_at IS NULL;

-- name: GetSenderID :one
SELECT * FROM control_plane.sender_ids WHERE customer_id = @customer_id AND id = @id;

-- name: MarkSenderIDsFirstUsed :exec
UPDATE control_plane.sender_ids s SET first_used_at = u.used_at
FROM (SELECT unnest(@customer_ids::uuid[]) AS customer_id, unnest(@addresses::text[]) AS address,
             unnest(@used_ats::timestamptz[]) AS used_at) u
WHERE s.customer_id = u.customer_id AND s.address = u.address AND s.first_used_at IS NULL;
