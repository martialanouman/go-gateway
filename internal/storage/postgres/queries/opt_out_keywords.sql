-- name: ListActiveOptOutKeywords :many
-- Every active opt-out keyword, for the MO STOP-detection snapshot (step-063) and the pipeline
-- opt-out stage. A disabled keyword must not trigger a suppression or auto-reply.
SELECT * FROM control_plane.opt_out_keywords WHERE status = 'active' ORDER BY country_code, keyword;

-- name: ListOptOutKeywords :many
-- Every opt-out keyword (active AND disabled), for the Admin list (step-064).
SELECT * FROM control_plane.opt_out_keywords ORDER BY country_code, keyword;

-- name: CreateOptOutKeyword :one
INSERT INTO control_plane.opt_out_keywords (country_code, keyword, action, match_type, auto_reply_template)
VALUES (
    sqlc.narg('country_code'),
    @keyword,
    @action,
    COALESCE(sqlc.narg('match_type')::text, 'exact'),
    sqlc.narg('auto_reply_template')
)
RETURNING *;

-- name: UpdateOptOutKeyword :one
UPDATE control_plane.opt_out_keywords SET
    keyword             = COALESCE(sqlc.narg('keyword'), keyword),
    action              = COALESCE(sqlc.narg('action'), action),
    match_type          = COALESCE(sqlc.narg('match_type'), match_type),
    auto_reply_template = COALESCE(sqlc.narg('auto_reply_template'), auto_reply_template),
    status              = COALESCE(sqlc.narg('status'), status),
    updated_at          = now()
WHERE id = @id
RETURNING *;

-- name: DeleteOptOutKeyword :execrows
DELETE FROM control_plane.opt_out_keywords WHERE id = @id;
