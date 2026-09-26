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

// TestAccountRepoCreateAndChannelRule exercises account creation against a real database and proves
// the channel CHECK constraint surfaces as a validation error, not a raw driver failure.
func TestAccountRepoCreateAndChannelRule(t *testing.T) {
	pool := pgtest.Pool(t)
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	ctx := context.Background()

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "AccountCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}

	acct, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app-1"})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if acct.Status != cp.AccountActive {
		t.Errorf("status = %q, want active", acct.Status)
	}
	if acct.AllowedBindTypes != cp.BindTRX {
		t.Errorf("allowed_bind_types = %q, want trx (the default)", acct.AllowedBindTypes)
	}
	if acct.MaxSessions != 1 {
		t.Errorf("max_sessions = %d, want 1 (the default)", acct.MaxSessions)
	}

	// Disabling both channels violates smpp_accounts_channel_ck -> validation_error.
	_, err = accounts.SetChannels(ctx, acct.ID, false, false)
	if code, _ := errs.CodeOf(err); code != errs.ErrValidation {
		t.Errorf("SetChannels(false,false) code = %q, want validation_error", code)
	}
}

// TestAccountRepoDuplicateNameConflicts proves the smpp_accounts_name_uq constraint becomes a
// conflict (409) for a second account with the same name under one customer.
func TestAccountRepoDuplicateNameConflicts(t *testing.T) {
	pool := pgtest.Pool(t)
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	ctx := context.Background()

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "DupCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	if _, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "same"}); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err = accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "same"})
	if code, _ := errs.CodeOf(err); code != errs.ErrConflict {
		t.Errorf("duplicate-name Create code = %q, want conflict", code)
	}
}

// TestAccountRepoCreateUnderUnknownCustomerIsValidation: a foreign-key violation (unknown customer)
// is a 422, matching the contract, which lists no 404 on create-smpp-account.
func TestAccountRepoCreateUnderUnknownCustomerIsValidation(t *testing.T) {
	pool := pgtest.Pool(t)
	accounts := postgres.NewAccountRepo(pool)

	_, err := accounts.Create(context.Background(), cp.NewAccount{CustomerID: uuid.New(), Name: "orphan"})
	if code, _ := errs.CodeOf(err); code != errs.ErrValidation {
		t.Errorf("orphan Create code = %q, want validation_error", code)
	}
}

// TestAccountRepoUpdateChangesThePoliciesCreateSets: sender_id_policy and the two optional SMPP ops were
// settable at creation only. Each is changed alone, so a COALESCE wired to the wrong column fails here.
func TestAccountRepoUpdateChangesThePoliciesCreateSets(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "PolicyCo-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	accounts := postgres.NewAccountRepo(pool)
	acct, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "policy"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	disabled, off := cp.SenderIDPolicyDisabled, false
	got, err := accounts.Update(ctx, acct.ID, cp.AccountPatch{SenderIDPolicy: &disabled})
	if err != nil || got.SenderIDPolicy != disabled || !got.QuerySMEnabled || !got.CancelSMEnabled || got.Name != "policy" {
		t.Fatalf("policy update = %+v err=%v, want disabled and every other field untouched", got, err)
	}
	if got, err = accounts.Update(ctx, acct.ID, cp.AccountPatch{CancelSMEnabled: &off}); err != nil || got.CancelSMEnabled || !got.QuerySMEnabled || got.SenderIDPolicy != disabled {
		t.Fatalf("cancel_sm update = %+v err=%v, want cancel_sm off alone", got, err)
	}
	if got, err = accounts.Update(ctx, acct.ID, cp.AccountPatch{QuerySMEnabled: &off}); err != nil || got.QuerySMEnabled || got.CancelSMEnabled {
		t.Fatalf("query_sm update = %+v err=%v, want query_sm off, cancel_sm still off", got, err)
	}
}
