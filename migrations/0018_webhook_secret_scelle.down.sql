-- step-295b: not reversible. This column expects a clear secret, and unsealing needs the master key
-- content-key-svc holds, which a migration cannot reach. So the down refuses a populated table exactly as
-- the up does, and is a plain rollback on an empty one — the only state in which the up could have run.
--
-- A refusal here leaves the schema at 18 and schema_migrations dirty at 17 (golang-migrate stamps the
-- TARGET before running), so the recovery is `go run ./cmd/migrate -store postgres force 18` — not the
-- force 17 the up's header names for its own failure.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM control_plane.webhooks) THEN
    RAISE EXCEPTION 'step-295b: cannot roll back with webhooks in place — this column expects a clear secret, and unsealing needs the master key that content-key-svc holds. Delete the webhooks first.';
  END IF;
END $$;

ALTER TABLE control_plane.webhooks
  DROP COLUMN secret_sealed,
  DROP COLUMN secret_kms_key_ref,
  ADD COLUMN secret text NOT NULL;
