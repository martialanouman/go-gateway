-- step-295: NOT reversible without loss, and the down migration says so rather than pretending.
--
-- Going back restores columns that cannot be filled: password_hash wants an argon2id hash of a password
-- this schema no longer holds in any recoverable form for a caller without the KMS, and auth_config_json
-- wants the credentials in clear. Re-creating them with a placeholder would be worse than failing — a
-- connector would look configured and bind with a password nobody chose.
--
-- So the down refuses on a populated table, exactly as the up does, and is a plain rollback on an empty
-- one (the only state in which the up could have run at all).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM control_plane.smsc_connectors) THEN
    RAISE EXCEPTION 'step-295: cannot roll back with connectors in place — a sealed password cannot be turned back into the argon2id hash this column expects. Delete the connectors first.';
  END IF;
  IF EXISTS (SELECT 1 FROM control_plane.external_billing_providers) THEN
    RAISE EXCEPTION 'step-295: cannot roll back with billing providers in place — rolling back would put their credentials back in clear. Delete the providers first.';
  END IF;
END $$;

ALTER TABLE control_plane.smsc_connectors
  DROP COLUMN password_sealed,
  DROP COLUMN password_kms_key_ref,
  ADD COLUMN password_hash text NOT NULL;

ALTER TABLE control_plane.external_billing_providers
  DROP COLUMN auth_config_sealed,
  DROP COLUMN auth_config_kms_key_ref,
  ADD COLUMN auth_config_json jsonb NOT NULL DEFAULT '{}'::jsonb;
