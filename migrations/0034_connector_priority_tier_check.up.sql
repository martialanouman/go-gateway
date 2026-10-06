-- priority_tier reserves a connector to the traffic categories of at least its rank (ADR-0020 §4, step-292):
-- 0 accepts all, 1 transactional and otp, 2 otp only. It had no bound until now, so a value already out of
-- range is clamped first, so the migration cannot fail: above 2, 2 is the closest meaning.
UPDATE control_plane.smsc_connectors
  SET priority_tier = LEAST(GREATEST(priority_tier, 0), 2)
  WHERE priority_tier NOT BETWEEN 0 AND 2;
ALTER TABLE control_plane.smsc_connectors ADD CONSTRAINT smsc_connectors_priority_tier_check
  CHECK (priority_tier BETWEEN 0 AND 2);
