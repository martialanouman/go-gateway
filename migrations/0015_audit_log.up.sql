-- step-290c: the consolidated operator audit trail. One row per audited Admin API request — every write,
-- and the reads that reveal subscriber numbers — written BEFORE the handler runs (no row, no action), its
-- outcome written after. It records who (a token fingerprint, never the token), which operation and which
-- resource path — never a body or a query string: an admin body carries the secrets revealed once (plan
-- §1.9). status NULL means the outcome was not recorded, not that the request succeeded. No code path deletes a row (spec: immutable, 1-7 years); the
-- down migration of THIS file does, which is the one way back and destroys a compliance trail.
CREATE TABLE control_plane.audit_log (
  id           uuid PRIMARY KEY DEFAULT uuidv7(),
  operator     text NOT NULL,
  operation_id text NOT NULL,
  method       text NOT NULL,
  target       text NOT NULL,
  request_id   text,
  status       smallint CHECK (status IS NULL OR status BETWEEN 100 AND 599),
  at           timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz
);
CREATE INDEX audit_log_at_idx ON control_plane.audit_log(at);
