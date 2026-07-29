-- Recreate billing_customers and the step-142c cross-table guards, then revert the customers columns.
CREATE TABLE control_plane.billing_customers (
  customer_id                  uuid PRIMARY KEY REFERENCES control_plane.customers(id) ON DELETE CASCADE,
  billing_mode                 text NOT NULL CHECK (billing_mode IN ('prepaid','postpaid')),
  overdraft_enabled            boolean NOT NULL DEFAULT false,
  overdraft_limit              integer CHECK (overdraft_limit IS NULL OR overdraft_limit >= 0),
  credit_limit                 integer CHECK (credit_limit IS NULL OR credit_limit >= 0),
  credit_limit_is_hard         boolean NOT NULL DEFAULT false,
  external_billing_provider_id uuid REFERENCES control_plane.external_billing_providers(id) ON DELETE SET NULL,
  updated_at                   timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT billing_customers_overdraft_limit_ck   CHECK (NOT overdraft_enabled OR overdraft_limit IS NOT NULL),
  CONSTRAINT billing_customers_hard_credit_limit_ck CHECK (NOT credit_limit_is_hard OR credit_limit IS NOT NULL)
);
CREATE TRIGGER billing_customers_touch BEFORE UPDATE ON control_plane.billing_customers
  FOR EACH ROW EXECUTE FUNCTION control_plane.touch_updated_at();

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

ALTER TABLE control_plane.customers
  DROP CONSTRAINT customers_account_scope_no_credit_ck,
  ADD CONSTRAINT customers_account_scope_no_overdraft_ck
    CHECK (balance_scope <> 'smpp_account' OR NOT overdraft_enabled),
  DROP CONSTRAINT customers_hard_credit_limit_ck,
  DROP COLUMN credit_limit,
  DROP COLUMN credit_limit_is_hard,
  DROP COLUMN external_billing_provider_id;
