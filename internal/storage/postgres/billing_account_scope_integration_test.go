package postgres_test

import (
	"context"
	"errors"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

func ptrInt(v int) *int { return &v }

// TestCustomerOverdraftForbiddenWhenAccountScoped proves the customers CHECK (step-142c): an account-scoped
// balance may not carry a customer-level overdraft (it would multiply the credit exposure across accounts).
// The violation surfaces as a translated validation error (422), not a raw pgx error.
func TestCustomerOverdraftForbiddenWhenAccountScoped(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()

	scope := cp.BalanceScopeSMPPAccount
	_, err := repo.Create(ctx, cp.NewCustomer{
		Name:             "account-scoped-overdraft",
		BalanceScope:     &scope,
		OverdraftEnabled: true,
		OverdraftLimit:   ptrInt(100),
	})
	if !errors.Is(err, errs.ErrValidation) {
		t.Fatalf("Create(account-scoped + overdraft) error = %v, want ErrValidation (forbidden combo)", err)
	}

	// The same customer WITHOUT overdraft is fine (account-scoped strict prepaid is allowed).
	if _, err := repo.Create(ctx, cp.NewCustomer{Name: "account-scoped-ok", BalanceScope: &scope}); err != nil {
		t.Fatalf("Create(account-scoped, no overdraft) error = %v, want nil", err)
	}
}

// TestBillingCustomersTriggerForbidsAccountScopedCredit proves the billing_customers trigger (step-142c):
// an overdraft OR a hard credit limit is rejected for an account-scoped customer, while strict prepaid and
// soft postpaid are accepted. billing_customers has no balance_scope column, so the trigger consults the
// owning customer.
func TestBillingCustomersTriggerForbidsAccountScopedCredit(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()

	scope := cp.BalanceScopeSMPPAccount
	acct, err := repo.Create(ctx, cp.NewCustomer{Name: "acct-scoped", BalanceScope: &scope})
	if err != nil {
		t.Fatalf("seed account-scoped customer: %v", err)
	}

	// Overdraft on an account-scoped customer → rejected by the trigger.
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.billing_customers (customer_id, billing_mode, overdraft_enabled, overdraft_limit)
		 VALUES ($1, 'prepaid', true, 100)`, acct.ID); err == nil {
		t.Error("INSERT overdraft billing_customers for account-scoped customer succeeded, want rejected")
	}
	// Hard credit limit on an account-scoped customer → rejected.
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.billing_customers (customer_id, billing_mode, credit_limit, credit_limit_is_hard)
		 VALUES ($1, 'postpaid', 100, true)`, acct.ID); err == nil {
		t.Error("INSERT hard-limit billing_customers for account-scoped customer succeeded, want rejected")
	}
	// Soft postpaid on an account-scoped customer → ALLOWED (advisory, never blocks).
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.billing_customers (customer_id, billing_mode, credit_limit, credit_limit_is_hard)
		 VALUES ($1, 'postpaid', 100, false)`, acct.ID); err != nil {
		t.Errorf("INSERT soft-postpaid billing_customers for account-scoped customer = %v, want allowed", err)
	}

	// The same overdraft on a CUSTOMER-scoped customer is fine.
	cust, err := repo.Create(ctx, cp.NewCustomer{Name: "cust-scoped"})
	if err != nil {
		t.Fatalf("seed customer-scoped customer: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.billing_customers (customer_id, billing_mode, overdraft_enabled, overdraft_limit)
		 VALUES ($1, 'prepaid', true, 100)`, cust.ID); err != nil {
		t.Errorf("INSERT overdraft billing_customers for customer-scoped customer = %v, want allowed", err)
	}
}

// TestFlipToAccountScopeRejectedWithCredit proves the reverse-direction guard (step-142c): a customer that
// already holds a hard credit limit in billing_customers cannot be flipped to account-scoped — the
// customers CHECK alone can't see credit_limit_is_hard (it lives only in billing_customers), so a trigger
// on the balance_scope update re-checks it. Without this, the flip would escape the ban (N× exposure).
func TestFlipToAccountScopeRejectedWithCredit(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()

	// Customer-scoped customer with a hard credit limit (allowed while customer-scoped).
	cust, err := repo.Create(ctx, cp.NewCustomer{Name: "flip-candidate"})
	if err != nil {
		t.Fatalf("seed customer: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.billing_customers (customer_id, billing_mode, credit_limit, credit_limit_is_hard)
		 VALUES ($1, 'postpaid', 1000, true)`, cust.ID); err != nil {
		t.Fatalf("seed hard-limit billing_customers: %v", err)
	}

	// Flipping this customer to account-scoped must be rejected (the hard limit would apply per-account).
	if _, err := pool.Exec(ctx,
		`UPDATE control_plane.customers SET balance_scope = 'smpp_account' WHERE id = $1`, cust.ID); err == nil {
		t.Error("flip to account-scoped with a hard credit limit succeeded, want rejected")
	}

	// A customer with no overdraft/hard-limit flips freely.
	plain, err := repo.Create(ctx, cp.NewCustomer{Name: "flip-ok"})
	if err != nil {
		t.Fatalf("seed plain customer: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE control_plane.customers SET balance_scope = 'smpp_account' WHERE id = $1`, plain.ID); err != nil {
		t.Errorf("flip plain customer to account-scoped = %v, want allowed", err)
	}
}
