DROP TRIGGER sender_ids_drop_rate_limit ON control_plane.sender_ids;
DROP FUNCTION control_plane.sender_id_drops_its_rate_limit();
DELETE FROM control_plane.rate_limits WHERE entity_type = 'sender_id';
ALTER TABLE control_plane.rate_limits DROP CONSTRAINT rate_limits_entity_type_check;
ALTER TABLE control_plane.rate_limits ADD CONSTRAINT rate_limits_entity_type_check
  CHECK (entity_type IN ('smpp_account','connector'));
