-- Forbid a customer-level overdraft / hard credit limit on an ACCOUNT-scoped balance (§6.9, step-142c).
-- When balance_scope=smpp_account each SMPP account has its OWN isolated balance, but the overdraft/credit
-- limit is a single CUSTOMER-level figure. Applying it to each account balance would multiply the
-- customer's credit exposure by the number of accounts (N accounts × overdraft_limit). An account-scoped
-- balance may therefore only be strict prepaid (floor 0) or soft postpaid (advisory, never blocks) — both
-- safe per account. Enforced at the DB so no write path (admin, future config-sync) can create the combo.

-- customers holds the admin-edited overdraft config alongside balance_scope in one row → a single CHECK.
ALTER TABLE control_plane.customers
  ADD CONSTRAINT customers_account_scope_no_overdraft_ck
    CHECK (balance_scope <> 'smpp_account' OR NOT overdraft_enabled);

-- billing_customers is the config the billing floor reads (step-142b) and the only place credit_limit_is_hard
-- lives; it carries no balance_scope, so a cross-table trigger consults the owning customer. Raising with
-- ERRCODE 23514 (check_violation) makes the repo translate it to a clean 422, like a table CHECK.
CREATE OR REPLACE FUNCTION control_plane.forbid_account_scope_credit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF (NEW.overdraft_enabled OR NEW.credit_limit_is_hard)
     AND (SELECT balance_scope FROM control_plane.customers WHERE id = NEW.customer_id) = 'smpp_account' THEN
    RAISE EXCEPTION 'overdraft/hard credit limit is not allowed on an account-scoped balance (customer %)', NEW.customer_id
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER billing_customers_no_account_scope_credit
  BEFORE INSERT OR UPDATE ON control_plane.billing_customers
  FOR EACH ROW EXECUTE FUNCTION control_plane.forbid_account_scope_credit();

-- Reverse direction: flipping a customer to account-scoped must re-check its billing_customers row, since
-- the customers CHECK only sees customers.overdraft_enabled — not credit_limit_is_hard (which lives only in
-- billing_customers). Without this, a customer-scoped customer holding a hard limit could switch to
-- account-scoped and escape the ban (N× exposure). Fires only when balance_scope is written.
CREATE OR REPLACE FUNCTION control_plane.forbid_account_scope_flip_with_credit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.balance_scope = 'smpp_account'
     AND EXISTS (SELECT 1 FROM control_plane.billing_customers
                 WHERE customer_id = NEW.id AND (overdraft_enabled OR credit_limit_is_hard)) THEN
    RAISE EXCEPTION 'cannot switch customer % to account-scoped: it has an overdraft or hard credit limit', NEW.id
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER customers_no_account_scope_flip_with_credit
  BEFORE UPDATE OF balance_scope ON control_plane.customers
  FOR EACH ROW EXECUTE FUNCTION control_plane.forbid_account_scope_flip_with_credit();
