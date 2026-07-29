-- Billing config integrity (§6.9, step-142b). A limit FLAG set without its VALUE is a misconfiguration
-- that the reserve floor mapping fails closed to strict prepaid — i.e. it silently cuts the customer off
-- (floor 0 on a postpaid/overdraft customer whose balance sits near 0 refuses every send). Reject the
-- misconfiguration at WRITE time instead, so an operator omission surfaces as a loud constraint error, not
-- a production outage: an enabled overdraft MUST carry an overdraft_limit, a hard credit limit MUST carry a
-- credit_limit.
ALTER TABLE control_plane.billing_customers
  ADD CONSTRAINT billing_customers_overdraft_limit_ck
    CHECK (NOT overdraft_enabled OR overdraft_limit IS NOT NULL),
  ADD CONSTRAINT billing_customers_hard_credit_limit_ck
    CHECK (NOT credit_limit_is_hard OR credit_limit IS NOT NULL);
