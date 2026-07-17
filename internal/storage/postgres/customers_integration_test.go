package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestCustomerRepoRoundTrip exercises create, read, update and delete against a real database, so
// the sqlc mapping and the pgtype conversions are validated end to end, not by hand.
func TestCustomerRepoRoundTrip(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()

	created, err := repo.Create(ctx, cp.NewCustomer{Name: "Acme"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.ID == uuid.Nil {
		t.Fatal("Create() returned the nil UUID")
	}
	if created.Status != cp.CustomerActive {
		t.Errorf("status = %q, want active (the schema default)", created.Status)
	}
	if created.BalanceScope != cp.BalanceScopeCustomer {
		t.Errorf("balance_scope = %q, want customer (the schema default)", created.BalanceScope)
	}

	got, err := repo.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Name != "Acme" {
		t.Errorf("Get().Name = %q, want Acme", got.Name)
	}

	name := "Acme Corp"
	updated, err := repo.Update(ctx, created.ID, cp.CustomerPatch{Name: &name})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.Name != "Acme Corp" {
		t.Errorf("Update().Name = %q, want Acme Corp", updated.Name)
	}

	if err := repo.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.Get(ctx, created.ID); !isNotFound(err) {
		t.Errorf("Get() after Delete error = %v, want not_found", err)
	}
}

// TestCustomerRepoGetMissingIsNotFound: a missing row is a translated not_found, not a raw pgx
// error.
func TestCustomerRepoGetMissingIsNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)

	_, err := repo.Get(context.Background(), uuid.New())
	if !isNotFound(err) {
		t.Errorf("Get(unknown) error = %v, want not_found", err)
	}
}

// TestCustomerRepoDeleteMissingIsNotFound: deleting nothing is not_found, so the handler can return
// a 404 rather than a silent 204.
func TestCustomerRepoDeleteMissingIsNotFound(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)

	if err := repo.Delete(context.Background(), uuid.New()); !isNotFound(err) {
		t.Errorf("Delete(unknown) error = %v, want not_found", err)
	}
}

// TestSuspendCustomerCascadesToItsAccounts is an M1 acceptance criterion: suspending a customer
// suspends every one of its accounts, in one transaction. The account is inserted with raw SQL
// because the accounts repository lands in a later step; the cascade under test is the customer
// repo's.
func TestSuspendCustomerCascadesToItsAccounts(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewCustomerRepo(pool)
	ctx := context.Background()

	customer, err := repo.Create(ctx, cp.NewCustomer{Name: "Cascade Co"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	var accountID uuid.UUID
	err = pool.QueryRow(ctx,
		`INSERT INTO control_plane.smpp_accounts (customer_id, name, status)
		 VALUES ($1, 'app-1', 'active') RETURNING id`, customer.ID).Scan(&accountID)
	if err != nil {
		t.Fatalf("insert account: %v", err)
	}

	suspended, err := repo.Suspend(ctx, customer.ID)
	if err != nil {
		t.Fatalf("Suspend() error = %v", err)
	}
	if suspended.Status != cp.CustomerSuspended {
		t.Errorf("customer status = %q, want suspended", suspended.Status)
	}

	var accountStatus string
	err = pool.QueryRow(ctx,
		`SELECT status FROM control_plane.smpp_accounts WHERE id = $1`, accountID).Scan(&accountStatus)
	if err != nil {
		t.Fatalf("read account status: %v", err)
	}
	if accountStatus != "suspended" {
		t.Errorf("account status = %q, want suspended — the suspend did not cascade", accountStatus)
	}
}

func isNotFound(err error) bool {
	code, ok := errs.CodeOf(err)
	return ok && code == errs.ErrNotFound
}
