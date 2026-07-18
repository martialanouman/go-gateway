package postgres_test

import (
	"context"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

func TestAPIKeyRepoPrincipalLookup(t *testing.T) {
	pool := pgtest.Pool(t)
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	credentials := postgres.NewCredentialRepo(pool)
	apikeys := postgres.NewAPIKeyRepo(pool)
	ctx := context.Background()

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "AuthCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "rest-app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	key, hash, err := credential.GenerateAPIKey()
	if err != nil {
		t.Fatalf("generate api key: %v", err)
	}
	if _, err := credentials.Create(ctx, cp.NewCredential{
		AccountID:  account.ID,
		Type:       cp.CredentialAPIKey,
		APIKeyHash: &hash,
	}); err != nil {
		t.Fatalf("create credential: %v", err)
	}

	// The presented key hashes to the stored hash and resolves to the right principal.
	principal, found, err := apikeys.PrincipalByAPIKeyHash(ctx, credential.HashAPIKey(key))
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !found {
		t.Fatal("expected the key to resolve to a principal")
	}
	if principal.AccountID != account.ID || principal.CustomerID != customer.ID {
		t.Errorf("principal ids: got account=%s customer=%s", principal.AccountID, principal.CustomerID)
	}
	if !principal.RESTEnabled {
		t.Error("rest_enabled should default true")
	}
	if principal.EffectiveStatus() != cp.AccountActive {
		t.Errorf("effective status: got %q want active", principal.EffectiveStatus())
	}
}

func TestAPIKeyRepoUnknownKey(t *testing.T) {
	pool := pgtest.Pool(t)
	apikeys := postgres.NewAPIKeyRepo(pool)

	_, found, err := apikeys.PrincipalByAPIKeyHash(context.Background(), credential.HashAPIKey("sgw_does_not_exist"))
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if found {
		t.Fatal("an unknown key must not resolve to a principal")
	}
}

func TestAPIKeyRepoReflectsRestDisabled(t *testing.T) {
	pool := pgtest.Pool(t)
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	credentials := postgres.NewCredentialRepo(pool)
	apikeys := postgres.NewAPIKeyRepo(pool)
	ctx := context.Background()

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "RestOffCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "smpp-only"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	// Turn REST off (SMPP stays on so the channel CHECK is satisfied).
	if _, err := accounts.SetChannels(ctx, account.ID, true, false); err != nil {
		t.Fatalf("set channels: %v", err)
	}

	_, hash, err := credential.GenerateAPIKey()
	if err != nil {
		t.Fatalf("generate api key: %v", err)
	}
	if _, err := credentials.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &hash,
	}); err != nil {
		t.Fatalf("create credential: %v", err)
	}

	// The key still resolves — the verifier, not the query, decides the 403. The row must report
	// rest_enabled = false so it can.
	principal, found, err := apikeys.PrincipalByAPIKeyHash(ctx, hash)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !found {
		t.Fatal("key should resolve even when REST is disabled (so the verifier can 403)")
	}
	if principal.RESTEnabled {
		t.Error("rest_enabled should be false after disabling the REST channel")
	}
}
