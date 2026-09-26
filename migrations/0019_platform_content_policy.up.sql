-- step-370: the platform default content-storage policy an `inherit` customer resolves to (§6.23). One row,
-- inserted here as 'off' — the constant it replaces — so the migration changes nothing an inherit customer
-- stores. The CHECK refuses 'stored_plaintext': §6.23 reserves storage in clear to a named customer under
-- contract, never to everyone who has not chosen. internal/adminapi/content_policy.go words the same list
-- for the dashboard: widen both together.
CREATE TABLE control_plane.platform_content_policy (
  id              boolean PRIMARY KEY DEFAULT true CHECK (id),
  content_storage text NOT NULL DEFAULT 'off' CHECK (content_storage IN ('off','stored_encrypted'))
);
INSERT INTO control_plane.platform_content_policy DEFAULT VALUES;
