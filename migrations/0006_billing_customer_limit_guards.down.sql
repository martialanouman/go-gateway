ALTER TABLE control_plane.billing_customers
  DROP CONSTRAINT billing_customers_overdraft_limit_ck,
  DROP CONSTRAINT billing_customers_hard_credit_limit_ck;
