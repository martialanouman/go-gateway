ALTER TABLE control_plane.smpp_accounts ADD COLUMN sender_id_policy text NOT NULL DEFAULT 'strict'
  CHECK (sender_id_policy IN ('strict','allow_unregistered_numeric','disabled'));
ALTER TABLE control_plane.sender_ids DROP COLUMN traffic_category;
