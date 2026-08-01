-- gdpr_erase_jobs tracks an on-demand RGPD erasure (§6.23, §14, step-166): who asked, for whom, and — once
-- done — the attestation, i.e. the PROOF of execution (scope, counters, completion time). The attestation
-- never carries erased content, only what was erased and how much (invariant a).
CREATE TABLE control_plane.gdpr_erase_jobs (
  id           uuid PRIMARY KEY DEFAULT uuidv7(),
  subject_type text NOT NULL CHECK (subject_type IN ('customer','msisdn')),
  subject_id   text NOT NULL,
  status       text NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued','running','completed','failed')),
  attestation  text,
  operator     text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz
);
CREATE INDEX gdpr_erase_jobs_created_idx ON control_plane.gdpr_erase_jobs(created_at);
