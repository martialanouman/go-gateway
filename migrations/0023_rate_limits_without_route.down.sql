ALTER TABLE control_plane.rate_limits DROP CONSTRAINT rate_limits_entity_type_check;
ALTER TABLE control_plane.rate_limits ADD CONSTRAINT rate_limits_entity_type_check
  CHECK (entity_type IN ('smpp_account','connector','route'));
