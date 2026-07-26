package postgres_test

import (
	"context"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestWebhookRepoGet proves Get returns a configured webhook and reports a clean absence (found=false)
// for an event type the account has not subscribed to.
func TestWebhookRepoGet(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "WebhookCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := postgres.NewAccountRepo(pool).Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "webhook-app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	// The Admin CRUD for webhooks is out of this step's scope, so seed the row directly.
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.webhooks (account_id, event_type, url, secret, retry_policy_json)
		 VALUES ($1, 'mo', 'https://example.test/hook', 'topsecret', '{"max_attempts":4}')`,
		account.ID); err != nil {
		t.Fatalf("seed webhook: %v", err)
	}

	repo := postgres.NewWebhookRepo(pool)

	got, found, err := repo.Get(ctx, account.ID, cp.WebhookEventMO)
	if err != nil {
		t.Fatalf("Get(mo): %v", err)
	}
	if !found {
		t.Fatal("Get(mo) found=false, want the seeded webhook")
	}
	if got.URL != "https://example.test/hook" || got.Secret != "topsecret" || got.Status != cp.WebhookActive {
		t.Errorf("webhook = %+v, want the seeded url/secret/active", got)
	}
	if string(got.RetryPolicyJSON) == "" {
		t.Error("retry_policy_json should carry the seeded policy")
	}

	if _, found, err := repo.Get(ctx, account.ID, cp.WebhookEventDLR); err != nil || found {
		t.Errorf("Get(dlr) = found %v err %v, want no webhook (found=false, nil err)", found, err)
	}
}
