-- name: CreateInboundKeyword :one
-- match_type and priority are inserted non-null: the handler resolves the DDL defaults ('prefix', 0)
-- before calling, so no column is ever left to a COALESCE here.
INSERT INTO control_plane.inbound_keywords (
    inbound_number_id, keyword, match_type, account_id, priority
) VALUES (
    @inbound_number_id, @keyword, @match_type, @account_id, @priority
)
RETURNING *;

-- name: ListAllInboundKeywords :many
-- Every active keyword across all shared numbers, for the MO router's in-memory snapshot (step-045).
-- Ordered by number then priority then id so the snapshot evaluates keywords in the right order.
SELECT * FROM control_plane.inbound_keywords
WHERE status = 'active'
ORDER BY inbound_number_id, priority, id;

-- name: ListInboundKeywords :many
-- Ordered by priority then id: priority drives MO evaluation (inbound_keywords_lookup_idx), and id is
-- a stable tie-break so equal priorities list deterministically.
SELECT * FROM control_plane.inbound_keywords
WHERE inbound_number_id = @inbound_number_id
ORDER BY priority, id;

-- name: UpdateInboundKeyword :one
-- Scoped by both keyword id and inbound_number_id: a keyword can only be patched within its own
-- number, so a mismatched pair matches no row (reported as ErrNotFound).
UPDATE control_plane.inbound_keywords SET
    keyword    = COALESCE(sqlc.narg('keyword'), keyword),
    match_type = COALESCE(sqlc.narg('match_type'), match_type),
    account_id = COALESCE(sqlc.narg('account_id'), account_id),
    priority   = COALESCE(sqlc.narg('priority'), priority),
    status     = COALESCE(sqlc.narg('status'), status)
WHERE id = @id AND inbound_number_id = @inbound_number_id
RETURNING *;

-- name: DeleteInboundKeyword :execrows
DELETE FROM control_plane.inbound_keywords
WHERE id = @id AND inbound_number_id = @inbound_number_id;
