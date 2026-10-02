-- A closed SMPP account can never leave closed (Admin PATCH, suspend, cascade, scripts).
CREATE FUNCTION control_plane.smpp_accounts_closed_is_final() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.status = 'closed' AND NEW.status <> 'closed' THEN
    RAISE EXCEPTION 'smpp account % is closed: status % refused', OLD.id, NEW.status
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER smpp_accounts_closed_is_final BEFORE UPDATE OF status ON control_plane.smpp_accounts
  FOR EACH ROW EXECUTE FUNCTION control_plane.smpp_accounts_closed_is_final();
