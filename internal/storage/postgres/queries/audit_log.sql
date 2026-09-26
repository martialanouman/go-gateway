-- name: InsertAuditIntent :one
-- Record an operator request before its handler runs: who, which operation, which path — never a body.
INSERT INTO control_plane.audit_log (operator, operation_id, method, target, request_id)
VALUES ($1, $2, $3, $4, $5)
RETURNING id;

-- name: FinishAudit :exec
-- Record the outcome of an audited request, once: an outcome already written is never replaced.
UPDATE control_plane.audit_log
   SET status = $2, finished_at = now()
 WHERE id = $1 AND status IS NULL;

-- name: ListAuditLog :many
-- Newest first, id breaking ties; every filter optional. Column-wise keyset (not a row-value comparison) so
-- sqlc infers each parameter's type.
SELECT * FROM control_plane.audit_log
WHERE (sqlc.narg(operator)::text IS NULL OR operator = sqlc.narg(operator))
  AND (sqlc.narg(from_at)::timestamptz IS NULL OR at >= sqlc.narg(from_at))
  AND (sqlc.narg(to_at)::timestamptz IS NULL OR at < sqlc.narg(to_at))
  AND (sqlc.narg(after_at)::timestamptz IS NULL
       OR at < sqlc.narg(after_at)
       OR (at = sqlc.narg(after_at) AND id < sqlc.narg(after_id)::uuid))
  -- Redundant with the line above, but an index can seek on it: without it every page rescans from the newest.
  AND (sqlc.narg(after_at)::timestamptz IS NULL OR at <= sqlc.narg(after_at))
ORDER BY at DESC, id DESC
LIMIT @lim;
