-- step-290d: the REST API key lookup had no index, while plan §1.9, the package doc of
-- internal/credential and internal/storage/postgres/authn.go all said it had one. Every authenticated
-- REST request — the surface sized for 8 000/s — scanned control_plane.credentials from end to end.
--
-- Two indexes, because PrincipalByAPIKeyHash matches an OR: the live hash, or the previous one inside a
-- rotation grace window (§6.3). With only the first, the planner still has to scan for the second branch.
--
-- Partial on NOT NULL, and the two predicates do NOT mean the same thing. api_key_hash is set only on an
-- api_key row (credentials_shape_ck says so). previous_secret_hash is outside that CHECK and holds
-- COALESCE(password_hash, api_key_hash) during a rotation — so on a bind row it holds an argon2id hash,
-- and the second index covers bind rows too. Harmless (the REST lookup is also filtered on
-- type = 'api_key'), and narrowing the predicate to api_key rows would be a behaviour change, not a
-- tidy-up: it would stop indexing the bind grace window.
--
-- Not UNIQUE, deliberately. Uniqueness would be a real property (a 256-bit key cannot collide), but
-- asserting it here would make this migration fail on any historical duplicate instead of making the
-- lookup fast, which is what it is for.
--
-- IF NOT EXISTS because an operator may well have created these by hand once the seq scan was noticed:
-- without it, migrate would fail 42P07 and leave version 16 dirty, blocking every later migration.
CREATE INDEX IF NOT EXISTS credentials_api_key_hash_idx
  ON control_plane.credentials(api_key_hash) WHERE api_key_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS credentials_previous_secret_hash_idx
  ON control_plane.credentials(previous_secret_hash) WHERE previous_secret_hash IS NOT NULL;
