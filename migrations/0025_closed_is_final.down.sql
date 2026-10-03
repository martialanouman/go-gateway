DROP TRIGGER smpp_accounts_closed_is_final ON control_plane.smpp_accounts;
DROP TRIGGER customers_closed_is_final ON control_plane.customers;
DROP FUNCTION control_plane.closed_is_final();
