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

// TestAccountScopeForbidsCreditConfig proves the same-table CHECK on customers (step-142c/d): an
// account-scoped balance may carry neither a customer-level overdraft nor a hard credit limit (both would
// apply per account and multiply the credit exposure). Since the billing config now lives on customers,
// this is one same-table CHECK — no cross-table triggers. Violations surface as a translated 422.
func TestAccountScopeForbidsCreditConfig(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()
	scope := cp.BalanceScopeSMPPAccount

	// Account-scoped + overdraft → rejected.
	if _, err := repo.Create(ctx, cp.NewCustomer{
		Name: "as-overdraft", BalanceScope: &scope, OverdraftEnabled: true, OverdraftLimit: ptrInt(100),
	}); !errors.Is(err, errs.ErrValidation) {
		t.Errorf("Create(account-scoped + overdraft) = %v, want ErrValidation", err)
	}
	// Account-scoped + hard credit limit → rejected.
	if _, err := repo.Create(ctx, cp.NewCustomer{
		Name: "as-hardlimit", BalanceScope: &scope, CreditLimit: ptrInt(100), CreditLimitIsHard: true,
	}); !errors.Is(err, errs.ErrValidation) {
		t.Errorf("Create(account-scoped + hard limit) = %v, want ErrValidation", err)
	}
	// Account-scoped + soft credit limit → ALLOWED (advisory, never blocks).
	if _, err := repo.Create(ctx, cp.NewCustomer{
		Name: "as-soft", BalanceScope: &scope, CreditLimit: ptrInt(100), CreditLimitIsHard: false,
	}); err != nil {
		t.Errorf("Create(account-scoped + soft limit) = %v, want allowed", err)
	}
	// Account-scoped strict prepaid → allowed.
	if _, err := repo.Create(ctx, cp.NewCustomer{Name: "as-strict", BalanceScope: &scope}); err != nil {
		t.Errorf("Create(account-scoped strict) = %v, want allowed", err)
	}
	// Customer-scoped with overdraft + hard limit → allowed (the ban is account-scope only).
	if _, err := repo.Create(ctx, cp.NewCustomer{
		Name: "cust-full", OverdraftEnabled: true, OverdraftLimit: ptrInt(100),
		CreditLimit: ptrInt(200), CreditLimitIsHard: true,
	}); err != nil {
		t.Errorf("Create(customer-scoped + overdraft + hard limit) = %v, want allowed", err)
	}
}

// TestFlipToAccountScopeRejectedWithCredit proves the flip direction is guarded by the SAME same-table
// CHECK (step-142d): a customer holding an overdraft or a hard credit limit cannot be switched to
// account-scoped — the CHECK re-validates on every customers UPDATE, so no trigger is needed.
func TestFlipToAccountScopeRejectedWithCredit(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()

	// Customer-scoped customer with a hard credit limit (allowed while customer-scoped).
	cust, err := repo.Create(ctx, cp.NewCustomer{
		Name: "flip-candidate", CreditLimit: ptrInt(1000), CreditLimitIsHard: true,
	})
	if err != nil {
		t.Fatalf("seed hard-limit customer: %v", err)
	}
	// Flipping to account-scoped must be rejected (the hard limit would apply per-account). balance_scope is
	// not a repo Update field, so the flip is a raw UPDATE — which the same-table CHECK still validates.
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
