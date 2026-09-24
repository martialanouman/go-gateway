-- name: ListSenderRewriteRules :many
-- The evaluation order of connector-pool (§6.16): scope precedence first, then priority (lower
-- first), then id. A sort on priority alone would let a platform rule outrank a connector rule.
SELECT * FROM control_plane.sender_id_rewrite_rules
WHERE (sqlc.narg('scope')::text IS NULL OR scope = sqlc.narg('scope'))
  AND (sqlc.narg('scope_id')::uuid IS NULL OR scope_id = sqlc.narg('scope_id'))
ORDER BY CASE scope WHEN 'connector' THEN 0 WHEN 'smpp_account' THEN 1 WHEN 'customer' THEN 2 ELSE 3 END,
         priority, id;

-- name: GetSenderRewriteRule :one
SELECT * FROM control_plane.sender_id_rewrite_rules WHERE id = @id;

-- name: CreateSenderRewriteRule :one
INSERT INTO control_plane.sender_id_rewrite_rules (
    scope, scope_id, match_sender_pattern, match_dest_pattern, rewrite_type, rewrite_to,
    fallback_pool_json, max_length, sanitize_charset_json, priority, reason
) VALUES (
    @scope, sqlc.narg('scope_id'), sqlc.narg('match_sender_pattern'), sqlc.narg('match_dest_pattern'),
    @rewrite_type, sqlc.narg('rewrite_to'), sqlc.narg('fallback_pool_json'), sqlc.narg('max_length'),
    sqlc.narg('sanitize_charset_json'), @priority, sqlc.narg('reason')
)
RETURNING *;

-- name: UpdateSenderRewriteRule :one
UPDATE control_plane.sender_id_rewrite_rules SET
    match_sender_pattern  = COALESCE(sqlc.narg('match_sender_pattern'), match_sender_pattern),
    match_dest_pattern    = COALESCE(sqlc.narg('match_dest_pattern'), match_dest_pattern),
    rewrite_type          = COALESCE(sqlc.narg('rewrite_type'), rewrite_type),
    rewrite_to            = COALESCE(sqlc.narg('rewrite_to'), rewrite_to),
    fallback_pool_json    = COALESCE(sqlc.narg('fallback_pool_json'), fallback_pool_json),
    max_length            = COALESCE(sqlc.narg('max_length'), max_length),
    sanitize_charset_json = COALESCE(sqlc.narg('sanitize_charset_json'), sanitize_charset_json),
    priority              = COALESCE(sqlc.narg('priority'), priority),
    reason                = COALESCE(sqlc.narg('reason'), reason),
    status                = COALESCE(sqlc.narg('status'), status),
    updated_at            = now()
WHERE id = @id
RETURNING *;

-- name: DeleteSenderRewriteRule :execrows
DELETE FROM control_plane.sender_id_rewrite_rules WHERE id = @id;
