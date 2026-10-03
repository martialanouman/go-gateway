-- A closed customer or SMPP account can never leave closed (Admin PATCH, suspend, cascade, scripts).
CREATE FUNCTION control_plane.closed_is_final() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.status = 'closed' AND NEW.status <> 'closed' THEN
    RAISE EXCEPTION '% % is closed: status % refused', TG_TABLE_NAME, OLD.id, NEW.status
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER customers_closed_is_final BEFORE UPDATE OF status ON control_plane.customers
  FOR EACH ROW EXECUTE FUNCTION control_plane.closed_is_final();
CREATE TRIGGER smpp_accounts_closed_is_final BEFORE UPDATE OF status ON control_plane.smpp_accounts
  FOR EACH ROW EXECUTE FUNCTION control_plane.closed_is_final();
