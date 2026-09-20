-- step-295 / ADR-0016: the two secrets the gateway REPLAYS to a third party move from a form that cannot
-- serve them — an argon2id hash for an outbound bind password, clear jsonb for the provider credentials —
-- to a sealed one, with the master-key reference beside it as content_keys.kms_key_ref does. The secrets
-- the gateway VERIFIES (credentials.password_hash, credentials.api_key_hash) are untouched and stay hashed.
--
-- NOT A CONVERSION. A hash does not decrypt, so there is nothing to carry over. The guards below refuse a
-- populated table rather than letting ADD COLUMN ... NOT NULL fail with a message naming a constraint
-- instead of the cause. The repository has never been deployed; a development database is recreated.
--
-- A failure here leaves the schema intact but schema_migrations dirty at 17: recover with
-- `go run ./cmd/migrate -store postgres force 16` after emptying the tables.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM control_plane.smsc_connectors) THEN
    RAISE EXCEPTION 'step-295: control_plane.smsc_connectors is not empty, and an argon2id password_hash cannot be converted into a sealed password. Delete the connectors and re-create them through the Admin API after migrating.';
  END IF;
  IF EXISTS (SELECT 1 FROM control_plane.external_billing_providers) THEN
    RAISE EXCEPTION 'step-295: control_plane.external_billing_providers is not empty. Its auth_config_json must be re-entered through the Admin API after migrating, so that it is sealed. Delete the providers and re-create them.';
  END IF;
END $$;

ALTER TABLE control_plane.smsc_connectors
  DROP COLUMN password_hash,
  ADD COLUMN password_sealed       bytea NOT NULL,
  ADD COLUMN password_kms_key_ref  text  NOT NULL;

-- No DEFAULT on either sealed column, on purpose: the sealed form of an empty document is not a
-- constant (the GCM nonce is drawn per call), so there is no literal a DEFAULT could hold. The Admin API
-- seals '{}' when a provider is created without credentials.
ALTER TABLE control_plane.external_billing_providers
  DROP COLUMN auth_config_json,
  ADD COLUMN auth_config_sealed      bytea NOT NULL,
  ADD COLUMN auth_config_kms_key_ref text  NOT NULL;
