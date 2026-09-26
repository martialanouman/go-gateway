DROP INDEX control_plane.audit_log_operator_at_idx;
GRANT DELETE, TRUNCATE ON control_plane.audit_log TO CURRENT_USER;
DROP TRIGGER audit_log_no_truncate ON control_plane.audit_log;
DROP TRIGGER audit_log_append_only ON control_plane.audit_log;
DROP FUNCTION control_plane.audit_log_append_only();
