-- step-290b: deliberately irreversible. The up migration replaced secrets with their fingerprint;
-- restoring them would mean keeping them. The statement below only gives golang-migrate something to run.
SELECT 1;
