-- name: CreateInboundNumber :one
INSERT INTO control_plane.inbound_numbers (
    address, number_type, country_code, mccmnc, connector_id, account_id
) VALUES (
    @address,
    @number_type,
    @country_code,
    sqlc.narg('mccmnc'),
    sqlc.narg('connector_id'),
    sqlc.narg('account_id')
)
RETURNING *;

-- name: ListInboundNumbers :many
SELECT * FROM control_plane.inbound_numbers ORDER BY id;

-- name: UpdateInboundNumber :one
UPDATE control_plane.inbound_numbers SET
    number_type  = COALESCE(sqlc.narg('number_type'), number_type),
    mccmnc       = COALESCE(sqlc.narg('mccmnc'), mccmnc),
    connector_id = COALESCE(sqlc.narg('connector_id'), connector_id),
    status       = COALESCE(sqlc.narg('status'), status)
WHERE id = @id
RETURNING *;

-- name: DeleteInboundNumber :execrows
DELETE FROM control_plane.inbound_numbers WHERE id = @id;

-- name: AssignInboundNumber :one
-- Sets account_id unconditionally (no COALESCE): a NULL clears the dedication so the number becomes
-- shared (keyword-resolved), which COALESCE would wrongly read as "leave unchanged".
UPDATE control_plane.inbound_numbers SET account_id = sqlc.narg('account_id')
WHERE id = @id
RETURNING *;
