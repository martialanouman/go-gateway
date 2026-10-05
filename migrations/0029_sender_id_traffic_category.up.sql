ALTER TABLE control_plane.sender_ids ADD COLUMN traffic_category text NOT NULL DEFAULT 'marketing'
  CONSTRAINT sender_ids_traffic_category_check CHECK (traffic_category IN ('otp','transactional','marketing'));
ALTER TABLE control_plane.smpp_accounts DROP COLUMN sender_id_policy;
