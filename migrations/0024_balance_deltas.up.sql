-- step-284 (ADR-0022): hot-path balance movements append here; billing-svc folds them into balances.
CREATE TABLE control_plane.balance_deltas (
  id         uuid NOT NULL DEFAULT uuidv7() PRIMARY KEY,
  owner_type text NOT NULL CHECK (owner_type IN ('customer','smpp_account')),
  owner_id   uuid NOT NULL,
  direction  text NOT NULL CHECK (direction IN ('mt','mo')),
  credits    integer NOT NULL CHECK (credits <> 0),
  created_at timestamptz NOT NULL DEFAULT now()
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);
CREATE INDEX balance_deltas_owner_idx ON control_plane.balance_deltas(owner_type, owner_id, direction);
