-- step-290d: the REST API key lookup had no index, while plan §1.9, the package doc of
-- internal/credential and internal/storage/postgres/authn.go all said it had one. Every authenticated
-- REST request — the surface sized for 8 000/s — scanned control_plane.credentials from end to end.
--
-- Two indexes, because PrincipalByAPIKeyHash matches an OR: the live hash, or the previous one inside a
-- rotation grace window (§6.3). With only the first, the planner still has to scan for the second branch.
-- Partial on NOT NULL: a bind credential holds neither column, and the shape CHECK guarantees it.
--
-- Not UNIQUE, deliberately. Uniqueness would be a real property (a 256-bit key cannot collide), but
-- asserting it here would make this migration fail on any historical duplicate instead of making the
-- lookup fast, which is what it is for.
CREATE INDEX credentials_api_key_hash_idx
  ON control_plane.credentials(api_key_hash) WHERE api_key_hash IS NOT NULL;
CREATE INDEX credentials_previous_secret_hash_idx
  ON control_plane.credentials(previous_secret_hash) WHERE previous_secret_hash IS NOT NULL;
