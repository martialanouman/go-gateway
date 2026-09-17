-- step-290d: reversible without loss — an index carries no data of its own. Removing it restores the
-- sequential scan every authenticated REST request paid before.
DROP INDEX IF EXISTS control_plane.credentials_api_key_hash_idx;
DROP INDEX IF EXISTS control_plane.credentials_previous_secret_hash_idx;
