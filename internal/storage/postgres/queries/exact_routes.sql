-- name: GetExactRoute :one
-- The exact route for one MSISDN, if any. A missing row means "no exact override" — the caller falls
-- back to normal route resolution, not an error.
SELECT msisdn, target_type, target_id, source, imported_at, updated_at
FROM control_plane.exact_routes
WHERE msisdn = @msisdn;

-- name: UpsertExactRoute :one
-- Idempotent by msisdn (the primary key): a repeated upsert of the same number overwrites the target,
-- source and imported_at, and refreshes updated_at. This is the single-row create/update path.
INSERT INTO control_plane.exact_routes (msisdn, target_type, target_id, source, imported_at)
VALUES (@msisdn, @target_type, @target_id, @source, sqlc.narg('imported_at'))
ON CONFLICT (msisdn) DO UPDATE SET
    target_type = EXCLUDED.target_type,
    target_id   = EXCLUDED.target_id,
    source      = EXCLUDED.source,
    imported_at = EXCLUDED.imported_at,
    updated_at  = now()
RETURNING msisdn, target_type, target_id, source, imported_at, updated_at;

-- name: DeleteExactRoute :execrows
DELETE FROM control_plane.exact_routes WHERE msisdn = @msisdn;

-- name: ListExactRoutes :many
-- A page of exact routes in msisdn order, keyset-paginated: pass the last msisdn of the previous page
-- as @after (the empty string for the first page, since every msisdn sorts after it). @lim is the page
-- size; callers request size+1 to detect a further page.
SELECT msisdn, target_type, target_id, source, imported_at, updated_at
FROM control_plane.exact_routes
WHERE msisdn > @after
ORDER BY msisdn
LIMIT @lim;

-- name: BatchUpsertExactRoute :batchexec
-- Bulk create/update for an import (MNP / carrier feed): the repo sends one pgx.Batch of these, one
-- round-trip, each row idempotent by msisdn so re-importing the same feed is safe.
INSERT INTO control_plane.exact_routes (msisdn, target_type, target_id, source, imported_at)
VALUES (@msisdn, @target_type, @target_id, @source, sqlc.narg('imported_at'))
ON CONFLICT (msisdn) DO UPDATE SET
    target_type = EXCLUDED.target_type,
    target_id   = EXCLUDED.target_id,
    source      = EXCLUDED.source,
    imported_at = EXCLUDED.imported_at,
    updated_at  = now();
