-- step-315: the audit trail becomes immutable by constraint. A row is written in two steps (intent, then
-- outcome), so the one allowed change is closing it: status and finished_at go from NULL to a value, every
-- other column unchanged. The trigger holds even for a superuser; the REVOKE holds the owner, which is the
-- application role today, in production where it is not one. step-297's purge is the only door to open.
CREATE FUNCTION control_plane.audit_log_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE'
     AND OLD.status IS NULL AND NEW.status IS NOT NULL
     AND NEW.id = OLD.id AND NEW.operator = OLD.operator AND NEW.operation_id = OLD.operation_id
     AND NEW.method = OLD.method AND NEW.target = OLD.target
     AND NEW.request_id IS NOT DISTINCT FROM OLD.request_id AND NEW.at = OLD.at THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'control_plane.audit_log is append-only: % refused', TG_OP
    USING ERRCODE = 'insufficient_privilege';
END $$;

CREATE TRIGGER audit_log_append_only BEFORE UPDATE OR DELETE ON control_plane.audit_log
  FOR EACH ROW EXECUTE FUNCTION control_plane.audit_log_append_only();
CREATE TRIGGER audit_log_no_truncate BEFORE TRUNCATE ON control_plane.audit_log
  FOR EACH STATEMENT EXECUTE FUNCTION control_plane.audit_log_append_only();

REVOKE DELETE, TRUNCATE ON control_plane.audit_log FROM CURRENT_USER;

CREATE INDEX audit_log_operator_at_idx ON control_plane.audit_log(operator, at);
