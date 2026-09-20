-- name: ListExternalProviders :many
-- Every external billing provider, ordered by name (§6.10). auth_config_sealed is returned as stored — sealed
-- (ADR-0016); the handler never unseals it, it returns a constant mask.
SELECT id, name, base_url, mode, cache_ttl_ms, sync_call_timeout_ms, failure_policy, status,
       created_at, updated_at, auth_config_sealed, auth_config_kms_key_ref
FROM control_plane.external_billing_providers
ORDER BY name;

-- name: GetExternalProvider :one
-- One provider by id, for the connectivity test to load its config. not_found if absent.
SELECT id, name, base_url, mode, cache_ttl_ms, sync_call_timeout_ms, failure_policy, status,
       created_at, updated_at, auth_config_sealed, auth_config_kms_key_ref
FROM control_plane.external_billing_providers
WHERE id = @id;

-- name: CreateExternalProvider :one
-- Insert a provider; nil cache_ttl_ms/failure_policy apply the schema defaults (1000 / fail_open). The
-- sealed credentials are NOT optional: the handler seals '{}' when none are given, because the sealed
-- form of an empty document is not a constant the DDL could default to.
INSERT INTO control_plane.external_billing_providers
  (name, base_url, auth_config_sealed, auth_config_kms_key_ref, mode, cache_ttl_ms, sync_call_timeout_ms, failure_policy)
VALUES
  (@name, @base_url, @auth_config_sealed, @auth_config_kms_key_ref, @mode,
   COALESCE(sqlc.narg('cache_ttl_ms'), 1000), sqlc.narg('sync_call_timeout_ms'),
   COALESCE(sqlc.narg('failure_policy'), 'fail_open'))
RETURNING id, name, base_url, mode, cache_ttl_ms, sync_call_timeout_ms, failure_policy, status,
          created_at, updated_at, auth_config_sealed, auth_config_kms_key_ref;

-- name: UpdateExternalProvider :one
-- Partial update: a NULL argument leaves its column unchanged (COALESCE). sync_call_timeout_ms cannot be
-- cleared back to NULL via COALESCE (a documented follow-up).
UPDATE control_plane.external_billing_providers SET
    name                 = COALESCE(sqlc.narg('name'), name),
    base_url             = COALESCE(sqlc.narg('base_url'), base_url),
    auth_config_sealed   = COALESCE(sqlc.narg('auth_config_sealed'), auth_config_sealed),
    auth_config_kms_key_ref = COALESCE(sqlc.narg('auth_config_kms_key_ref'), auth_config_kms_key_ref),
    mode                 = COALESCE(sqlc.narg('mode'), mode),
    cache_ttl_ms         = COALESCE(sqlc.narg('cache_ttl_ms'), cache_ttl_ms),
    sync_call_timeout_ms = COALESCE(sqlc.narg('sync_call_timeout_ms'), sync_call_timeout_ms),
    failure_policy       = COALESCE(sqlc.narg('failure_policy'), failure_policy),
    status               = COALESCE(sqlc.narg('status'), status),
    updated_at           = now()
WHERE id = @id
RETURNING id, name, base_url, mode, cache_ttl_ms, sync_call_timeout_ms, failure_policy, status,
          created_at, updated_at, auth_config_sealed, auth_config_kms_key_ref;

-- name: DeleteExternalProvider :execrows
-- Delete a provider. customers.external_billing_provider_id references it ON DELETE SET NULL, so deleting a
-- provider in use simply unassigns it from its customers.
DELETE FROM control_plane.external_billing_providers WHERE id = @id;
