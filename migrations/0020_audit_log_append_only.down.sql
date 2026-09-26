DROP INDEX control_plane.audit_log_operator_at_idx;
DO $$ BEGIN
  EXECUTE format('GRANT DELETE, TRUNCATE ON control_plane.audit_log TO %s',
    (SELECT relowner::regrole FROM pg_class WHERE oid = 'control_plane.audit_log'::regclass));
END $$;
DROP TRIGGER audit_log_no_truncate ON control_plane.audit_log;
DROP TRIGGER audit_log_append_only ON control_plane.audit_log;
DROP FUNCTION control_plane.audit_log_append_only();
