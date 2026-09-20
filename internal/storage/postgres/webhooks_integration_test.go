package postgres_test

import (
	"context"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestWebhookRepoGetActive proves GetActive returns a configured webhook and reports a clean absence (found=false)
// for an event type the account has not subscribed to.
func TestWebhookRepoGetActive(t *testing.T) {
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

	got, found, err := repo.GetActive(ctx, account.ID, cp.WebhookEventMO)
	if err != nil {
		t.Fatalf("GetActive(mo): %v", err)
	}
	if !found {
		t.Fatal("GetActive(mo) found=false, want the seeded webhook")
	}
	if got.URL != "https://example.test/hook" || got.Secret != "topsecret" || got.Status != cp.WebhookActive {
		t.Errorf("webhook = %+v, want the seeded url/secret/active", got)
	}
	if string(got.RetryPolicyJSON) == "" {
		t.Error("retry_policy_json should carry the seeded policy")
	}

	if _, found, err := repo.GetActive(ctx, account.ID, cp.WebhookEventDLR); err != nil || found {
		t.Errorf("GetActive(dlr) = found %v err %v, want no webhook (found=false, nil err)", found, err)
	}
}

// TestWebhookRepoGetActiveSkipsADisabledWebhook proves disabling is what an operator thinks it is: the
// delivery paths stop resolving the webhook at all. The rule lives in the query rather than in each
// caller because there are two of them and one had forgotten it — the deferred retry runner kept
// pushing to a URL the operator had switched off, for as long as its attempt budget and its six-hour
// age bound allowed.
func TestWebhookRepoGetActiveSkipsADisabledWebhook(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "DisabledHookCo"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := postgres.NewAccountRepo(pool).Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "disabled-app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.webhooks (account_id, event_type, url, secret, status)
		 VALUES ($1, 'mo', 'https://example.test/off', 'topsecret', 'disabled')`,
		account.ID); err != nil {
		t.Fatalf("seed disabled webhook: %v", err)
	}

	got, found, err := postgres.NewWebhookRepo(pool).GetActive(ctx, account.ID, cp.WebhookEventMO)
	if err != nil {
		t.Fatalf("GetActive(mo): %v", err)
	}
	if found {
		t.Errorf("a disabled webhook was resolved for delivery: %+v", got)
	}
}
