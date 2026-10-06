ALTER TABLE control_plane.rate_limits DROP CONSTRAINT rate_limits_entity_type_check;
ALTER TABLE control_plane.rate_limits ADD CONSTRAINT rate_limits_entity_type_check
  CHECK (entity_type IN ('smpp_account','connector','sender_id'));

-- entity_id is polymorphic, so no FK can cascade; a sender ID also goes with its customer
-- (ON DELETE CASCADE), which no application path sees.
CREATE FUNCTION control_plane.sender_id_drops_its_rate_limit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM control_plane.rate_limits WHERE entity_type = 'sender_id' AND entity_id = OLD.id;
  RETURN OLD;
END $$;
CREATE TRIGGER sender_ids_drop_rate_limit AFTER DELETE ON control_plane.sender_ids
  FOR EACH ROW EXECUTE FUNCTION control_plane.sender_id_drops_its_rate_limit();
