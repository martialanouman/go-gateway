DO $$ BEGIN
  EXECUTE format('REVOKE DELETE ON control_plane.audit_log FROM %s',
    (SELECT relowner::regrole FROM pg_class WHERE oid = 'control_plane.audit_log'::regclass));
END $$;
CREATE OR REPLACE FUNCTION control_plane.audit_log_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE'
     AND OLD.status IS NULL AND NEW.status IS NOT NULL AND NEW.finished_at = now()
     AND NEW.id = OLD.id AND NEW.operator = OLD.operator AND NEW.operation_id = OLD.operation_id
     AND NEW.method = OLD.method AND NEW.target = OLD.target
     AND NEW.request_id IS NOT DISTINCT FROM OLD.request_id AND NEW.at = OLD.at THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'control_plane.audit_log is append-only: % refused', TG_OP
    USING ERRCODE = 'insufficient_privilege';
END $$;
