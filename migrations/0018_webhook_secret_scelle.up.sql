-- step-295b / ADR-0016: webhooks.secret is the third secret the gateway REPLAYS — webhook.Sign needs it in
-- clear on every delivery — so it is sealed, not hashed.
--
-- Sealing needs the master key content-key-svc holds, which a migration cannot reach: there is no way to
-- convert the existing rows here. The guard refuses a populated table rather than letting ADD COLUMN ...
-- NOT NULL fail with a message naming a constraint instead of the cause. The repository has never been
-- deployed; a development database is recreated.
--
-- A failure here leaves the schema intact but schema_migrations dirty at 18: recover with
-- `go run ./cmd/migrate -store postgres force 17` after emptying the table.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM control_plane.webhooks) THEN
    RAISE EXCEPTION 'step-295b: control_plane.webhooks is not empty, and sealing a secret needs the master key that content-key-svc holds, which a migration cannot reach. Delete the webhooks and re-create them through the Admin API after migrating, which seals the secret on write.';
  END IF;
END $$;

-- No DEFAULT: the sealed form of a value is not a constant, the GCM nonce being drawn per call.
ALTER TABLE control_plane.webhooks
  DROP COLUMN secret,
  ADD COLUMN secret_sealed      bytea NOT NULL,
  ADD COLUMN secret_kms_key_ref text  NOT NULL;
