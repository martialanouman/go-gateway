package postgres_test

import (
	"context"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestAnOperatorCanProvisionEverythingNeededToSend walks the M1 acceptance path end to end against a
// real PostgreSQL: customer -> smpp_account -> credentials (api key + bind) -> connector -> static
// route -> sender ID, then suspends the customer and confirms the cascade. It sends no message — M1
// stops exactly here (plan §1) — but everything a message needs exists when it returns.
func TestAnOperatorCanProvisionEverythingNeededToSend(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)
	connectors := postgres.NewConnectorRepo(pool)
	routes := postgres.NewRouteRepo(pool)
	senders := postgres.NewSenderIDRepo(pool)

	// 1. Customer.
	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "Provision Co"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}

	// 2. SMPP account.
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app-prod"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	// 3. Credentials: one API key and one SMPP bind.
	apiHash := "api-key-hash"
	if _, err := creds.Create(ctx, cp.NewCredential{AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &apiHash}); err != nil {
		t.Fatalf("create api key: %v", err)
	}
	sysID, bindHash := "sys-prod", "bind-hash"
	if _, err := creds.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialSMPPBind, SystemID: &sysID, PasswordHash: &bindHash,
	}); err != nil {
		t.Fatalf("create bind: %v", err)
	}

	// 4. Connector.
	connector, err := connectors.Create(ctx, cp.NewConnector{
		Name: "carrier-a", Host: "smsc.carrier", Port: 2775, BindType: cp.BindTRX, SystemID: "esme", PasswordHash: "hash",
	})
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}

	// 5. Static route to that connector.
	if _, err := routes.Create(ctx, cp.NewRoute{
		Name: "to-carrier-a", DistributionStrategy: cp.DistributionStatic, TargetConnectorID: &connector.ID,
	}); err != nil {
		t.Fatalf("create route: %v", err)
	}

	// 6. Sender ID.
	if _, err := senders.Create(ctx, cp.NewSenderID{CustomerID: customer.ID, Address: "ACME"}); err != nil {
		t.Fatalf("create sender id: %v", err)
	}

	// 7. Suspend the customer; the account's effective status becomes suspended (cascade).
	if _, err := customers.Suspend(ctx, customer.ID); err != nil {
		t.Fatalf("suspend customer: %v", err)
	}
	got, err := accounts.Get(ctx, account.ID)
	if err != nil {
		t.Fatalf("get account after suspend: %v", err)
	}
	effective := cp.EffectiveAccountStatus(cp.CustomerSuspended, got.Status)
	if effective != cp.AccountSuspended {
		t.Errorf("effective account status = %q, want suspended after the customer cascade", effective)
	}
}
