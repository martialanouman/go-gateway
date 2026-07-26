-- name: ListSuppressions :many
-- Every suppression, for the opt-out Bloom snapshot (step-061): each scope's filter is seeded from
-- these rows at boot. The msisdn is already E.164-normalized at write.
SELECT * FROM control_plane.suppressions;

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
