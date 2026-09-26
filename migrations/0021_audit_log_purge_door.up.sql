-- step-297: the retention purge is the one door in the append-only trigger (ADR-0018). The floor is in hours,
-- as absolute as the purge's own cutoff: '1 year' or '365 days' follow the session's calendar (leap day,
-- daylight saving), and one row the trigger judged too young would refuse the whole purge.
CREATE OR REPLACE FUNCTION control_plane.audit_log_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' AND current_setting('audit_log.purge', true) = 'on'
     AND OLD.at < now() - interval '8760 hours' THEN
    RETURN OLD;
  END IF;
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

DO $$ BEGIN
  EXECUTE format('GRANT DELETE ON control_plane.audit_log TO %s',
    (SELECT relowner::regrole FROM pg_class WHERE oid = 'control_plane.audit_log'::regclass));
END $$;
