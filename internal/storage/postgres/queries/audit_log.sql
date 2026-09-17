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
