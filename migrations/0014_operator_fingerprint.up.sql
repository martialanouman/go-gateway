-- step-290b: the operator columns recorded the admin bearer token itself (auth.StaticVerifier used it as
-- the principal's subject). Rewrite every recorded token into auth.Fingerprint: 'tok_' and the first 16 hex
-- characters of its SHA-256. The Go and SQL computations must agree; the integration test
-- TestOperatorFingerprintMigrationMatchesGo holds them together.
--
-- Left alone: 'unknown' (no principal), and any value already shaped like a fingerprint, so re-running these
-- statements never hashes a fingerprint twice. The cost: a raw token that is itself exactly 'unknown' or
-- 'tok_' + 16 lowercase hex stays as it is.
--
-- Not covered: the deployment job runs this BEFORE the rollout, so the old pods still serving can record
-- raw tokens after it commits. The statements are idempotent — re-run them once the rollout completes.
-- Tokens already copied into application logs are not reachable from here either: rotate the admin tokens.
UPDATE control_plane.content_access_audit
   SET operator = 'tok_' || left(encode(sha256(convert_to(operator, 'UTF8')), 'hex'), 16)
 WHERE operator <> 'unknown' AND operator !~ '^tok_[0-9a-f]{16}$';

UPDATE control_plane.gdpr_erase_jobs
   SET operator = 'tok_' || left(encode(sha256(convert_to(operator, 'UTF8')), 'hex'), 16)
 WHERE operator <> 'unknown' AND operator !~ '^tok_[0-9a-f]{16}$';

UPDATE control_plane.message_export_jobs
   SET operator = 'tok_' || left(encode(sha256(convert_to(operator, 'UTF8')), 'hex'), 16)
 WHERE operator <> 'unknown' AND operator !~ '^tok_[0-9a-f]{16}$';
