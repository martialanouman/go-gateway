-- name: CreateCredential :one
-- The hashes are computed by internal/credential before this runs. The shape (system_id +
-- password_hash for a bind, api_key_hash for a key) is enforced by credentials_shape_ck; a second
-- credential of the same type on the account violates credentials_one_per_type_uq -> 409.
INSERT INTO control_plane.credentials (account_id, type, system_id, password_hash, api_key_hash)
VALUES (@account_id, @type, sqlc.narg('system_id'), sqlc.narg('password_hash'), sqlc.narg('api_key_hash'))
RETURNING *;

-- name: ListCredentialsByAccount :many
SELECT * FROM control_plane.credentials WHERE account_id = @account_id ORDER BY type;

-- name: GetCredential :one
SELECT * FROM control_plane.credentials WHERE account_id = @account_id AND id = @id;

-- name: SetCredentialStatus :one
UPDATE control_plane.credentials SET status = @status
WHERE account_id = @account_id AND id = @id
RETURNING *;

-- name: RotateCredential :one
-- Writes the freshly hashed secret into the column that matches the credential's type. When a grace
-- period is given, the OLD hash (visible on the right-hand side of the UPDATE) is preserved in
-- previous_secret_hash and grace_expires_at is set now()+grace, so the previous secret keeps working
-- in parallel until it expires; a null grace is an immediate cutover.
UPDATE control_plane.credentials SET
    password_hash = CASE WHEN type = 'smpp_bind' THEN @new_hash ELSE password_hash END,
    api_key_hash  = CASE WHEN type = 'api_key'   THEN @new_hash ELSE api_key_hash END,
    previous_secret_hash = CASE
        WHEN sqlc.narg('grace_seconds')::int IS NOT NULL THEN COALESCE(password_hash, api_key_hash)
        ELSE NULL END,
    grace_expires_at = CASE
        WHEN sqlc.narg('grace_seconds')::int IS NOT NULL
            THEN now() + make_interval(secs => sqlc.narg('grace_seconds')::int)
        ELSE NULL END,
    rotated_at = now()
WHERE account_id = @account_id AND id = @id
RETURNING *;

-- name: GetAPIKeyPrincipal :one
-- REST authentication lookup (§1.9): the presented key is SHA-256 hashed by internal/credential,
-- then found by that hash on the single api_key credential. The rotation grace window is honoured —
-- the previous secret keeps working until grace_expires_at. Only the credential status is gated
-- here (a revoked key is simply invalid); rest_enabled and the account/customer statuses are
-- RETURNED, not filtered, so the verifier can answer with the right code (401 vs 403) rather than a
-- blanket "not found".
SELECT
    a.id          AS account_id,
    a.customer_id AS customer_id,
    a.status      AS account_status,
    a.rest_enabled AS rest_enabled,
    c.status      AS customer_status
FROM control_plane.credentials cr
JOIN control_plane.smpp_accounts a ON a.id = cr.account_id
JOIN control_plane.customers c ON c.id = a.customer_id
WHERE cr.type = 'api_key'
  AND cr.status = 'active'
  AND (
        cr.api_key_hash = @api_key_hash
     OR (cr.previous_secret_hash = @api_key_hash
         AND cr.grace_expires_at IS NOT NULL
         AND cr.grace_expires_at > now())
  );
