-- name: InsertContentAccess :exec
-- Record one content:read access to a message body (fact only, never the plaintext).
INSERT INTO control_plane.content_access_audit (operator, message_id, customer_id, outcome)
VALUES ($1, $2, $3, $4);
