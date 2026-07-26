-- name: ListActiveAntispamRules :many
-- Every active anti-spam rule, for the router's cold-loaded evaluation snapshot (step-065). Ordered
-- by scope so the engine indexes account, customer and global rules deterministically.
SELECT * FROM control_plane.antispam_rules WHERE status = 'active' ORDER BY scope, id;

-- name: ListAntispamRules :many
-- Every anti-spam rule, active and disabled, for the Admin list (step-067).
SELECT * FROM control_plane.antispam_rules ORDER BY scope, id;

-- name: GetAntispamRule :one
SELECT * FROM control_plane.antispam_rules WHERE id = @id;

-- name: CreateAntispamRule :one
-- A scope/scope_id mismatch violates antispam_scope_ck (global -> scope_id NULL) -> 422.
INSERT INTO control_plane.antispam_rules (rule_type, scope, scope_id, config_json, action)
VALUES (@rule_type, @scope, sqlc.narg('scope_id'), @config_json, @action)
RETURNING *;

-- name: UpdateAntispamRule :one
UPDATE control_plane.antispam_rules SET
    config_json = COALESCE(sqlc.narg('config_json'), config_json),
    action      = COALESCE(sqlc.narg('action'), action),
    status      = COALESCE(sqlc.narg('status'), status),
    updated_at  = now()
WHERE id = @id
RETURNING *;

-- name: DeleteAntispamRule :execrows
DELETE FROM control_plane.antispam_rules WHERE id = @id;
