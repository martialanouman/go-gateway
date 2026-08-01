-- message_export_jobs tracks an asynchronous CDR export (§15, step-187): who asked, over which window and
-- filters, and — once done — where the artefact landed and how many rows it holds.
--
-- The row records the REQUEST and the OUTCOME, never a message: no body (invariant a) and no MSISDN. The
-- filters are stored as the JSON the operator submitted, so an export is reproducible and auditable; a
-- msisdn predicate can appear there, which is why the column is documented as request metadata rather than
-- searchable data, and why nothing joins on it.
CREATE TABLE control_plane.message_export_jobs (
  id            uuid PRIMARY KEY DEFAULT uuidv7(),
  status        text NOT NULL DEFAULT 'queued'
                  CHECK (status IN ('queued','running','completed','failed')),
  format        text NOT NULL CHECK (format IN ('csv','jsonl')),
  -- masked records what the ARTEFACT contains, not what the caller asked: an unmasked export requires
  -- msisdn:reveal, so the flag is the durable proof of which one was produced.
  masked        boolean NOT NULL DEFAULT true,
  filters       jsonb NOT NULL,
  row_count     integer,
  -- artefact_uri is where the file landed (file:// today; an object URI once the infrastructure provides
  -- one). NULL until the job completes.
  artefact_uri  text,
  -- error explains a failed job — a row cap exceeded, a store fault. A failed status with no reason is not
  -- actionable, so the column is part of the contract's ExportJob.
  error         text,
  operator      text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz NOT NULL,
  finished_at   timestamptz
);
CREATE INDEX message_export_jobs_created_idx ON control_plane.message_export_jobs(created_at);
