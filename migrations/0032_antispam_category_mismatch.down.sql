DELETE FROM control_plane.antispam_rules WHERE rule_type = 'category_mismatch';
ALTER TABLE control_plane.antispam_rules DROP CONSTRAINT antispam_rules_rule_type_check;
ALTER TABLE control_plane.antispam_rules ADD CONSTRAINT antispam_rules_rule_type_check
  CHECK (rule_type IN ('velocity','content_blacklist','duplicate','reputation'));
