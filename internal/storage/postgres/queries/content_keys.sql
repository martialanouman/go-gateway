-- name: GetActiveContentKey :one
-- The customer's current active content key (at most one, guaranteed by content_keys_one_active_idx).
SELECT * FROM control_plane.content_keys
WHERE customer_id = $1 AND status = 'active';

-- name: GetContentKeyByID :one
SELECT * FROM control_plane.content_keys WHERE id = $1;

-- name: ListContentKeysByCustomer :many
-- Every key of a customer, newest first — active plus the retired history still needed to decrypt old CDRs.
SELECT * FROM control_plane.content_keys
WHERE customer_id = $1
ORDER BY created_at DESC, id DESC;

-- name: InsertContentKey :one
-- Insert a new active key. The partial unique index rejects a second active key for the same customer.
INSERT INTO control_plane.content_keys (customer_id, wrapped_key, kms_key_ref)
VALUES ($1, $2, $3)
RETURNING *;

-- name: RetireActiveContentKey :execrows
-- Demote the customer's active key to retired (it stays decryptable). Zero rows when there is no active key.
UPDATE control_plane.content_keys
SET status = 'retired', retired_at = now()
WHERE customer_id = $1 AND status = 'active';

-- name: LockCustomerForContentKey :one
-- Serialize concurrent GetOrCreate/Rotate on a customer by locking its row for the transaction.
SELECT id FROM control_plane.customers WHERE id = $1 FOR UPDATE;

-- name: SetCustomerContentKey :execrows
-- Point the customer at its current active content key (control_plane.customers.content_key_id).
UPDATE control_plane.customers SET content_key_id = $2 WHERE id = $1;
