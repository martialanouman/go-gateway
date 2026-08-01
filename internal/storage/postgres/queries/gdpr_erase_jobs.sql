-- name: CreateGDPREraseJob :one
-- Queue an erasure. The job row is the durable record of the request, written before any erasure starts.
INSERT INTO control_plane.gdpr_erase_jobs (subject_type, subject_id, operator)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetGDPREraseJob :one
SELECT * FROM control_plane.gdpr_erase_jobs WHERE id = $1;

-- name: MarkGDPREraseJobRunning :exec
UPDATE control_plane.gdpr_erase_jobs SET status = 'running' WHERE id = $1 AND status = 'queued';

-- name: FinishGDPREraseJob :exec
-- Close a job with its outcome and attestation (the proof of execution, never erased content).
UPDATE control_plane.gdpr_erase_jobs
SET status = $2, attestation = $3, finished_at = now()
WHERE id = $1;
