-- priority_flag_default is sent on the wire since step-292 and writable at the Admin API since step-294:
-- bound it to the SMPP range here too, since SQL was the only way to set it before. A value already out of
-- range is clamped first, to what the pool was sending (it clamped on read), so the migration cannot fail.
UPDATE control_plane.smsc_connectors
  SET priority_flag_default = LEAST(GREATEST(priority_flag_default, 0), 3)
  WHERE priority_flag_default NOT BETWEEN 0 AND 3;
ALTER TABLE control_plane.smsc_connectors ADD CONSTRAINT smsc_connectors_priority_flag_default_check
  CHECK (priority_flag_default BETWEEN 0 AND 3);
