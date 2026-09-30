-- step-283: the route bucket is gone. Nothing enforces it any more, so a row that remains is refused here
-- rather than silently ignored: the migration fails and an operator decides what the limit becomes.
ALTER TABLE control_plane.rate_limits DROP CONSTRAINT rate_limits_entity_type_check;
ALTER TABLE control_plane.rate_limits ADD CONSTRAINT rate_limits_entity_type_check
  CHECK (entity_type IN ('smpp_account','connector'));
