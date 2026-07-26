-- name: ListSuppressions :many
-- Every suppression, for the opt-out Bloom snapshot (step-061): each scope's filter is seeded from
-- these rows at boot. The msisdn is already E.164-normalized at write.
SELECT * FROM control_plane.suppressions;

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
