-- name: CreateRoutingScript :one
-- Create a routing script. It always starts as a draft (publish is a separate transactional step, so
-- creation never contends the one-active-per-scope index). checksum is computed by the caller.
INSERT INTO control_plane.routing_scripts
    (scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by)
VALUES (@scope, sqlc.narg('scope_id'), @name, @language, @source_code, @checksum, 'draft', @timeout_ms,
        sqlc.narg('max_instructions'), sqlc.narg('max_memory_kb'), sqlc.narg('created_by'))
RETURNING id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at;

-- name: GetRoutingScript :one
SELECT id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at
FROM control_plane.routing_scripts
WHERE id = @id;

-- name: GetActiveRoutingScript :one
-- The single active script for a scope, if any. NULLS NOT DISTINCT matching so the platform scope
-- (scope_id NULL) resolves. Used by the runtime's scope resolution (step-110).
SELECT id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at
FROM control_plane.routing_scripts
WHERE scope = @scope AND scope_id IS NOT DISTINCT FROM sqlc.narg('scope_id') AND status = 'active';

-- name: UpdateRoutingScript :one
-- Update a script's editable fields (draft edits). checksum is recomputed by the caller from the new
-- source. Status transitions go through the dedicated publish path, not here.
UPDATE control_plane.routing_scripts SET
    name = @name, language = @language, source_code = @source_code, checksum = @checksum,
    timeout_ms = @timeout_ms, max_instructions = sqlc.narg('max_instructions'), max_memory_kb = sqlc.narg('max_memory_kb')
WHERE id = @id
RETURNING id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at;

-- name: DeleteRoutingScript :execrows
DELETE FROM control_plane.routing_scripts WHERE id = @id;

-- name: ListRoutingScriptVersions :many
-- Every version for a (scope, scope_id), newest first — the version history for one target.
SELECT id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at
FROM control_plane.routing_scripts
WHERE scope = @scope AND scope_id IS NOT DISTINCT FROM sqlc.narg('scope_id')
ORDER BY created_at DESC, id DESC;

-- name: ListRoutingScripts :many
-- A page of all routing scripts, keyset-paginated by id.
SELECT id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at
FROM control_plane.routing_scripts
WHERE id > @after
ORDER BY id
LIMIT @lim;

-- name: DemoteActiveRoutingScript :execrows
-- Disable the current active script for a scope, so a new one can be promoted without violating the
-- one-active-per-scope unique index. Run inside the publish transaction, before PromoteRoutingScript.
UPDATE control_plane.routing_scripts SET status = 'disabled'
WHERE scope = @scope AND scope_id IS NOT DISTINCT FROM sqlc.narg('scope_id') AND status = 'active';

-- name: PromoteRoutingScript :one
-- Promote a script to active and stamp published_at. Paired with DemoteActiveRoutingScript in the
-- publish transaction.
UPDATE control_plane.routing_scripts SET status = 'active', published_at = now()
WHERE id = @id
RETURNING id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at;

-- name: ListActiveRoutingScripts :many
-- Every active script across all scopes, to build the router's immutable script snapshot (step-110).
SELECT id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at
FROM control_plane.routing_scripts
WHERE status = 'active'
ORDER BY scope, scope_id;

-- name: AssignRoutingScript :one
-- Reassign a DRAFT to a different scope, atomically (the status='draft' guard closes the handler's
-- read-then-write race: an active script can never be reassigned even if it is published concurrently).
-- Only the scope/scope_id change; publish is a separate step.
UPDATE control_plane.routing_scripts SET scope = @scope, scope_id = sqlc.narg('scope_id')
WHERE id = @id AND status = 'draft'
RETURNING id, scope, scope_id, name, language, source_code, checksum, status, timeout_ms, max_instructions, max_memory_kb, created_by, created_at, published_at;
