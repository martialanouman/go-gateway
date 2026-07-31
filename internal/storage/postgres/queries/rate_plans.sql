-- name: ListRatePlans :many
-- Every rate plan, ordered by name. Rate plans are few (per-operator pricing), so the list is unpaginated
-- (the contract returns a bare array).
SELECT id, name, credits_per_segment_mt_json, credits_per_segment_mo_json, billing_mode, charge_on, status,
       created_at, updated_at
FROM control_plane.rate_plans
ORDER BY name;

-- name: CreateRatePlan :one
-- Insert a rate plan; nil billing_mode/charge_on apply the schema defaults (either / submission).
INSERT INTO control_plane.rate_plans
  (name, credits_per_segment_mt_json, credits_per_segment_mo_json, billing_mode, charge_on)
VALUES
  (@name, @credits_per_segment_mt_json, @credits_per_segment_mo_json,
   COALESCE(sqlc.narg('billing_mode'), 'either'), COALESCE(sqlc.narg('charge_on'), 'submission'))
RETURNING id, name, credits_per_segment_mt_json, credits_per_segment_mo_json, billing_mode, charge_on, status,
          created_at, updated_at;

-- name: UpdateRatePlan :one
-- Partial update: a NULL argument leaves its column unchanged (COALESCE).
UPDATE control_plane.rate_plans SET
    name                        = COALESCE(sqlc.narg('name'), name),
    credits_per_segment_mt_json = COALESCE(sqlc.narg('credits_per_segment_mt_json'), credits_per_segment_mt_json),
    credits_per_segment_mo_json = COALESCE(sqlc.narg('credits_per_segment_mo_json'), credits_per_segment_mo_json),
    billing_mode                = COALESCE(sqlc.narg('billing_mode'), billing_mode),
    charge_on                   = COALESCE(sqlc.narg('charge_on'), charge_on),
    status                      = COALESCE(sqlc.narg('status'), status),
    updated_at                  = now()
WHERE id = @id
RETURNING id, name, credits_per_segment_mt_json, credits_per_segment_mo_json, billing_mode, charge_on, status,
          created_at, updated_at;

-- name: DeleteRatePlan :execrows
-- Delete a rate plan. customers.rate_plan_id references it with no ON DELETE clause (RESTRICT), so a plan
-- still assigned to a customer raises a foreign-key violation the caller maps to a 409 conflict.
DELETE FROM control_plane.rate_plans WHERE id = @id;
