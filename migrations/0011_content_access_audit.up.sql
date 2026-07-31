-- content_access_audit records every content:read access to a decrypted message body (§14, step-163): who,
-- which message, when, and the outcome. It stores only the FACT of access, never the plaintext (invariant a).
CREATE TABLE control_plane.content_access_audit (
  id          uuid PRIMARY KEY DEFAULT uuidv7(),
  operator    text NOT NULL,
  message_id  uuid NOT NULL,
  customer_id uuid,
  outcome     text NOT NULL
                CHECK (outcome IN ('granted','unreadable','not_found')),
  accessed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX content_access_audit_message_idx ON control_plane.content_access_audit(message_id);
CREATE INDEX content_access_audit_accessed_idx ON control_plane.content_access_audit(accessed_at);
