-- name: CreateRoute :one
INSERT INTO control_plane.routes (
    name, priority, match_account_id, match_customer_id, match_sender_pattern,
    match_dest_pattern, match_content_pattern, distribution_strategy,
    target_connector_id, fallback_route_id
) VALUES (
    @name,
    COALESCE(sqlc.narg('priority')::int, 100),
    sqlc.narg('match_account_id'),
    sqlc.narg('match_customer_id'),
    sqlc.narg('match_sender_pattern'),
    sqlc.narg('match_dest_pattern'),
    sqlc.narg('match_content_pattern'),
    @distribution_strategy,
    sqlc.narg('target_connector_id'),
    sqlc.narg('fallback_route_id')
)
RETURNING *;

-- name: GetRoute :one
SELECT * FROM control_plane.routes WHERE id = @id;

-- name: ListRoutes :many
-- Ordered by priority (lower first), the evaluation order of the router.
SELECT * FROM control_plane.routes ORDER BY priority, id;

-- name: UpdateRoute :one
UPDATE control_plane.routes SET
    name                  = COALESCE(sqlc.narg('name'), name),
    priority              = COALESCE(sqlc.narg('priority'), priority),
    match_account_id      = COALESCE(sqlc.narg('match_account_id'), match_account_id),
    match_customer_id     = COALESCE(sqlc.narg('match_customer_id'), match_customer_id),
    match_sender_pattern  = COALESCE(sqlc.narg('match_sender_pattern'), match_sender_pattern),
    match_dest_pattern    = COALESCE(sqlc.narg('match_dest_pattern'), match_dest_pattern),
    match_content_pattern = COALESCE(sqlc.narg('match_content_pattern'), match_content_pattern),
    distribution_strategy = COALESCE(sqlc.narg('distribution_strategy'), distribution_strategy),
    target_connector_id   = COALESCE(sqlc.narg('target_connector_id'), target_connector_id),
    fallback_route_id     = COALESCE(sqlc.narg('fallback_route_id'), fallback_route_id),
    status                = COALESCE(sqlc.narg('status'), status)
WHERE id = @id
RETURNING *;

-- name: DeleteRoute :execrows
DELETE FROM control_plane.routes WHERE id = @id;

-- name: ListRouteTargets :many
SELECT * FROM control_plane.route_targets WHERE route_id = @route_id ORDER BY priority, connector_id;

-- name: InsertRouteTarget :exec
INSERT INTO control_plane.route_targets (route_id, connector_id, weight, priority)
VALUES (@route_id, @connector_id, COALESCE(sqlc.narg('weight')::int, 1), COALESCE(sqlc.narg('priority')::int, 0));

-- name: DeleteRouteTargets :exec
DELETE FROM control_plane.route_targets WHERE route_id = @route_id;
