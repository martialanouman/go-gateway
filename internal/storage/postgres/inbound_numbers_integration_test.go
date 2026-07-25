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

// TestInboundNumberRepoCRUD walks create -> list -> update against PostgreSQL, checking the DDL
// defaults (status 'active') come back and a partial update leaves untouched fields alone.
func TestInboundNumberRepoCRUD(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewInboundNumberRepo(pool)
	ctx := context.Background()

	mccmnc := "20801"
	created, err := repo.Create(ctx, cp.NewInboundNumber{
		Address:     "36000",
		NumberType:  cp.NumberShortcode,
		CountryCode: "FR",
		MCCMNC:      &mccmnc,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.Status != cp.InboundNumberActive {
		t.Errorf("status = %q, want active (the DDL default)", created.Status)
	}
	if created.MCCMNC == nil || *created.MCCMNC != "20801" {
		t.Errorf("mccmnc = %v, want 20801", created.MCCMNC)
	}
	if created.AccountID != nil {
		t.Errorf("account_id = %v, want nil (shared by default)", created.AccountID)
	}

	list, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(list) != 1 || list[0].ID != created.ID {
		t.Errorf("List() = %+v, want the one created number", list)
	}

	disabled := cp.InboundNumberDisabled
	updated, err := repo.Update(ctx, created.ID, cp.InboundNumberPatch{Status: &disabled})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updated.Status != cp.InboundNumberDisabled {
		t.Errorf("status = %q, want disabled", updated.Status)
	}
	// Address, number_type and mccmnc must survive a status-only patch.
	if updated.Address != "36000" || updated.NumberType != cp.NumberShortcode {
		t.Errorf("identity fields changed on a status-only patch: %+v", updated)
	}
	if updated.MCCMNC == nil || *updated.MCCMNC != "20801" {
		t.Errorf("mccmnc = %v, want 20801 preserved", updated.MCCMNC)
	}
}

// TestInboundNumberRepoDuplicateConflicts proves the inbound_numbers_uq (address, country_code)
// constraint becomes a conflict (409) on a second number with the same address and country.
func TestInboundNumberRepoDuplicateConflicts(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewInboundNumberRepo(pool)
	ctx := context.Background()

	base := cp.NewInboundNumber{Address: "36001", NumberType: cp.NumberLongcode, CountryCode: "FR"}
	if _, err := repo.Create(ctx, base); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := repo.Create(ctx, base)
	if code, _ := errs.CodeOf(err); code != errs.ErrConflict {
		t.Errorf("duplicate (address, country_code) Create code = %q, want conflict", code)
	}

	// A different country_code for the same address is allowed (the key is the pair).
	if _, err := repo.Create(ctx, cp.NewInboundNumber{Address: "36001", NumberType: cp.NumberLongcode, CountryCode: "BE"}); err != nil {
		t.Errorf("same address / different country should not conflict, got %v", err)
	}
}

// TestInboundNumberRepoDeleteMissing reports ErrNotFound (404) when nothing matched.
func TestInboundNumberRepoDeleteMissing(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewInboundNumberRepo(pool)
	ctx := context.Background()

	err := repo.Delete(ctx, uuid.New())
	if code, _ := errs.CodeOf(err); code != errs.ErrNotFound {
		t.Errorf("Delete(unknown) code = %q, want not_found", code)
	}
}

// TestInboundNumberRepoAssignDedicatedAndShared proves assign both dedicates a number to an account
// and, with a nil account, clears the dedication back to shared (NULL) — the case COALESCE would get
// wrong.
func TestInboundNumberRepoAssignDedicatedAndShared(t *testing.T) {
	pool := pgtest.Pool(t)
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	repo := postgres.NewInboundNumberRepo(pool)
	ctx := context.Background()

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "InboundCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	acct, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "inbound-app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	num, err := repo.Create(ctx, cp.NewInboundNumber{Address: "36002", NumberType: cp.NumberShortcode, CountryCode: "FR"})
	if err != nil {
		t.Fatalf("create inbound number: %v", err)
	}

	// Dedicate.
	dedicated, err := repo.Assign(ctx, num.ID, &acct.ID)
	if err != nil {
		t.Fatalf("Assign(dedicate) error = %v", err)
	}
	if dedicated.AccountID == nil || *dedicated.AccountID != acct.ID {
		t.Errorf("account_id = %v, want %v after dedication", dedicated.AccountID, acct.ID)
	}

	// Clear back to shared.
	shared, err := repo.Assign(ctx, num.ID, nil)
	if err != nil {
		t.Fatalf("Assign(clear) error = %v", err)
	}
	if shared.AccountID != nil {
		t.Errorf("account_id = %v, want nil (shared) after clearing", shared.AccountID)
	}
}

// TestInboundNumberRepoAssignMissing reports ErrNotFound (404) for an unknown id.
func TestInboundNumberRepoAssignMissing(t *testing.T) {
	pool := pgtest.Pool(t)
	repo := postgres.NewInboundNumberRepo(pool)
	ctx := context.Background()

	_, err := repo.Assign(ctx, uuid.New(), nil)
	if code, _ := errs.CodeOf(err); code != errs.ErrNotFound {
		t.Errorf("Assign(unknown) code = %q, want not_found", code)
	}
}
