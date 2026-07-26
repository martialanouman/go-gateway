-- name: ListSuppressions :many
-- Every suppression, for the opt-out Bloom snapshot (step-061): each scope's filter is seeded from
-- these rows at boot. The msisdn is already E.164-normalized at write.
SELECT * FROM control_plane.suppressions;

-- name: CreateSuppressionReturning :one
-- Admin create (step-064): NOT idempotent — a duplicate violates suppressions_uq -> 409 (unlike the
-- MO STOP path, which dedups). Returns the created row.
INSERT INTO control_plane.suppressions (scope, scope_id, msisdn, source, reason)
VALUES (@scope, sqlc.narg('scope_id'), @msisdn, @source, sqlc.narg('reason'))
RETURNING *;

-- name: ListSuppressionsPage :many
-- Keyset pagination on the UUIDv7 id, with optional scope / scope_id / msisdn filters (step-064). The
-- caller asks for limit+1 rows to learn whether a further page exists.
SELECT * FROM control_plane.suppressions
WHERE (sqlc.narg('scope')::text    IS NULL OR scope    = sqlc.narg('scope'))
  AND (sqlc.narg('scope_id')::uuid IS NULL OR scope_id = sqlc.narg('scope_id'))
  AND (sqlc.narg('msisdn')::text   IS NULL OR msisdn   = sqlc.narg('msisdn'))
  AND (sqlc.narg('after')::uuid    IS NULL OR id       > sqlc.narg('after'))
ORDER BY id
LIMIT @lim;

-- name: DeleteSuppressionByID :execrows
DELETE FROM control_plane.suppressions WHERE id = @id;

-- name: ImportSuppressions :execrows
-- Idempotent bulk insert (step-064): every msisdn in one statement, ON CONFLICT DO NOTHING against
-- suppressions_uq. Returns the number of NEW rows inserted (duplicates are skipped). Every msisdn
-- must already be E.164-normalized (the CHECK rejects a non-canonical value, failing the whole batch).
INSERT INTO control_plane.suppressions (scope, scope_id, msisdn, source)
SELECT @scope, sqlc.narg('scope_id'), m, @source
FROM unnest(@msisdns::text[]) AS m
ON CONFLICT ON CONSTRAINT suppressions_uq DO NOTHING;

-- name: CreateSuppression :execrows
-- Idempotent write (step-063): a repeated STOP for the same (scope, scope_id, msisdn) does not
-- duplicate. ON CONFLICT targets suppressions_uq (NULLS NOT DISTINCT), so the platform scope
-- (scope_id NULL) dedups too. Returns rows affected: 0 = already suppressed.
INSERT INTO control_plane.suppressions (scope, scope_id, msisdn, source, reason)
VALUES (@scope, sqlc.narg('scope_id'), @msisdn, @source, sqlc.narg('reason'))
ON CONFLICT ON CONSTRAINT suppressions_uq DO NOTHING;

-- name: DeleteSuppressionByKey :execrows
-- Remove a suppression by its natural key (step-063 unsuppress / START). scope_id compared with IS
-- NOT DISTINCT FROM so the platform scope matches a NULL argument.
DELETE FROM control_plane.suppressions
WHERE scope = @scope AND scope_id IS NOT DISTINCT FROM sqlc.narg('scope_id') AND msisdn = @msisdn;

-- name: IsSuppressed :one
-- Exact confirmation of a Bloom hit (step-061). scope_id is compared with IS NOT DISTINCT FROM so the
-- platform scope (scope_id NULL) matches a NULL argument, mirroring suppressions_uq's NULLS NOT
-- DISTINCT.
SELECT EXISTS (
  SELECT 1 FROM control_plane.suppressions
  WHERE scope = @scope
    AND scope_id IS NOT DISTINCT FROM sqlc.narg('scope_id')
    AND msisdn = @msisdn
) AS suppressed;
