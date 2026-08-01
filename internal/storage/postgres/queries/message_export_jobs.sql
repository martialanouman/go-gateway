-- name: CreateMessageExportJob :one
-- Queue an export. The job row is the durable record of the request, written before any row is read, so a
-- worker that dies mid-way leaves a visibly stuck job rather than a silently dropped request.
INSERT INTO control_plane.message_export_jobs (format, masked, filters, operator, expires_at)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetMessageExportJob :one
SELECT * FROM control_plane.message_export_jobs WHERE id = $1;

-- name: MarkMessageExportJobRunning :exec
UPDATE control_plane.message_export_jobs SET status = 'running' WHERE id = $1 AND status = 'queued';

-- name: CompleteMessageExportJob :exec
-- Close a job that produced an artefact: where it landed and how many rows it holds.
UPDATE control_plane.message_export_jobs
SET status = 'completed', artefact_uri = $2, row_count = $3, finished_at = now()
WHERE id = $1;

-- name: FailMessageExportJob :exec
-- Close a job that produced nothing, with the reason. A failed status with no reason is not actionable.
UPDATE control_plane.message_export_jobs
SET status = 'failed', error = $2, finished_at = now()
WHERE id = $1;
