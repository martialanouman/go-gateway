-- name: CreateSenderID :one
-- The customer_id comes from the path; an unknown one violates the FK -> 422. A duplicate address
-- for the customer violates sender_ids_uq -> 409.
INSERT INTO control_plane.sender_ids (customer_id, address, created_by)
VALUES (@customer_id, @address, sqlc.narg('created_by'))
RETURNING *;

-- name: ListSenderIDsByCustomer :many
SELECT * FROM control_plane.sender_ids WHERE customer_id = @customer_id ORDER BY address;

-- name: UpdateSenderID :one
UPDATE control_plane.sender_ids SET status = COALESCE(sqlc.narg('status'), status)
WHERE customer_id = @customer_id AND id = @id
RETURNING *;

-- name: DeleteSenderID :execrows
DELETE FROM control_plane.sender_ids WHERE customer_id = @customer_id AND id = @id;
