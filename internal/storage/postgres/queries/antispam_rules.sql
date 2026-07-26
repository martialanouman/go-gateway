-- name: ListActiveAntispamRules :many
-- Every active anti-spam rule, for the router's cold-loaded evaluation snapshot (step-065). Ordered
-- by scope so the engine indexes account, customer and global rules deterministically.
SELECT * FROM control_plane.antispam_rules WHERE status = 'active' ORDER BY scope, id;
