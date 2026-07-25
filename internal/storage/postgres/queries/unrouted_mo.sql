-- name: CreateUnroutedMO :one
-- Records an MO that resolved to no account. Never stores the message body (invariant a).
INSERT INTO control_plane.unrouted_mo (
    received_at, connector_id, inbound_number_id, source_addr, dest_addr, segment_count, encoding, reason
) VALUES (
    @received_at, sqlc.narg('connector_id'), sqlc.narg('inbound_number_id'),
    @source_addr, @dest_addr, @segment_count, @encoding, @reason
)
RETURNING *;

-- name: ListUnroutedMOFirst :many
-- First page: newest first, id breaks ties (matches unrouted_mo_page_idx).
SELECT * FROM control_plane.unrouted_mo
ORDER BY received_at DESC, id DESC
LIMIT @lim;

-- name: ListUnroutedMOAfter :many
-- Keyset page after (received_at, id): strictly older rows, same order. Column-wise (not a row-value
-- comparison) so sqlc infers each parameter's type correctly (timestamptz, then uuid).
SELECT * FROM control_plane.unrouted_mo
WHERE received_at < @after_received_at
   OR (received_at = @after_received_at AND id < @after_id)
ORDER BY received_at DESC, id DESC
LIMIT @lim;
