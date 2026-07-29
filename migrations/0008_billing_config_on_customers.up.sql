-- Consolidate the MT billing config onto control_plane.customers (step-142d, option A / ADR-0010). The
-- reserve-floor config was split between customers (admin-edited, populated) and billing_customers (read by
-- the floor, never populated) — the floor read a dead table. customers is now the SINGLE source of truth:
-- move the only floor fields it lacked (credit_limit, credit_limit_is_hard, external_billing_provider_id)
-- onto it and drop billing_customers. With credit_limit_is_hard on customers, the account-scope ban becomes
-- a single same-table CHECK — the two step-142c cross-table triggers are no longer needed.

ALTER TABLE control_plane.customers
  ADD COLUMN credit_limit                 integer CHECK (credit_limit IS NULL OR credit_limit >= 0),  -- postpaid MT limit
  ADD COLUMN credit_limit_is_hard         boolean NOT NULL DEFAULT false,                             -- hard = a reserve floor
  ADD COLUMN external_billing_provider_id uuid REFERENCES control_plane.external_billing_providers(id) ON DELETE SET NULL,
  -- A hard credit limit needs its value; the flag alone would fail closed and cut the customer off.
  ADD CONSTRAINT customers_hard_credit_limit_ck CHECK (NOT credit_limit_is_hard OR credit_limit IS NOT NULL);

-- Replace the overdraft-only account-scope ban with one that also covers the (now same-table) hard limit:
-- an account-scoped balance may carry neither an overdraft nor a hard credit limit (both would apply per
-- account and multiply the customer's exposure). Same-table CHECK → every customers write re-validates it.
ALTER TABLE control_plane.customers
  DROP CONSTRAINT customers_account_scope_no_overdraft_ck,
  ADD CONSTRAINT customers_account_scope_no_credit_ck
    CHECK (balance_scope <> 'smpp_account' OR (NOT overdraft_enabled AND NOT credit_limit_is_hard));

-- The step-142c cross-table guards existed only because credit_limit_is_hard lived in a different table;
-- with everything on customers the same-table CHECK above subsumes them.
DROP TRIGGER customers_no_account_scope_flip_with_credit ON control_plane.customers;
DROP FUNCTION control_plane.forbid_account_scope_flip_with_credit();
DROP TRIGGER billing_customers_no_account_scope_credit ON control_plane.billing_customers;
DROP FUNCTION control_plane.forbid_account_scope_credit();

DROP TABLE control_plane.billing_customers;
