-- step-295 / ADR-0016: two secrets were stored in a form that cannot serve their purpose.
--
-- smsc_connectors.password_hash held an argon2id hash of the password of an OUTBOUND bind. SMPP v3.4
-- §4.1.1 puts that password in clear in the bind_transceiver PDU, and a hash does not un-hash: the
-- column could never serve the one thing it existed for. That is why connector-pool-svc reads
-- CONNECTOR_PASSWORD from the environment instead, and why a rotation through the Admin API answered
-- 200 while the bind knew nothing about it.
--
-- external_billing_providers.auth_config_json held the third-party credentials in CLEAR jsonb — so in
-- every backup and every replica. The Admin API masks it on read, which protects the HTTP response and
-- nothing else.
--
-- Both are now SEALED by content-key-svc (ConfigSecrets.Seal), and the key reference is stored beside
-- the ciphertext so an operator can tell which master key a row belongs to and a future KEK rotation
-- knows what it has to re-seal — exactly as content_keys.kms_key_ref does.
--
-- The secrets the gateway VERIFIES rather than replays are untouched and stay hashed:
-- credentials.password_hash (inbound bind) and credentials.api_key_hash are correct as they are.
--
-- NOT A CONVERSION. An argon2id hash cannot be decrypted, so there is nothing to carry over: a stored
-- connector password is simply gone. The guards below refuse to run on a populated table instead of
-- letting ADD COLUMN ... NOT NULL fail with a message that names a constraint rather than the cause.
-- The repository has never been deployed; a development database is recreated, not migrated.
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
