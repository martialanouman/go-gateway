-- step-295b / ADR-0016: the THIRD secret the gateway replays to a third party. webhook.Sign needs this
-- HMAC-SHA256 key in clear on every MO/DLR delivery, so it cannot be hashed — and step-295, whose
-- inventory started from passwords and API keys, did not find it. It was still `text` in clear, which put
-- every customer's signing key in any backup, replica or dump of control_plane.webhooks.
--
-- NOT A CONVERSION, for a different reason than 0017's. A clear secret COULD be sealed, but not by SQL:
-- sealing needs the master key held by content-key-svc, which golang-migrate cannot reach. So the guard
-- below refuses a populated table rather than letting ADD COLUMN ... NOT NULL fail with a message naming
-- a constraint instead of the cause. The repository has never been deployed; a development database is
-- recreated.
--
-- A failure here leaves the schema intact but schema_migrations dirty at 18: recover with
-- `go run ./cmd/migrate -store postgres force 17` after emptying the table.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM control_plane.webhooks) THEN
    RAISE EXCEPTION 'step-295b: control_plane.webhooks is not empty, and sealing a secret needs the master key that content-key-svc holds, which a migration cannot reach. Delete the webhooks and re-create them through the Admin API after migrating, which seals the secret on write.';
  END IF;
END $$;

-- No DEFAULT on either column, as in 0017: the sealed form of a value is not a constant (the GCM nonce is
-- drawn per call), so there is no literal a DEFAULT could hold. A webhook has no secret until the Admin
-- API seals the one the operator supplied.
ALTER TABLE control_plane.webhooks
  DROP COLUMN secret,
  ADD COLUMN secret_sealed      bytea NOT NULL,
  ADD COLUMN secret_kms_key_ref text  NOT NULL;
