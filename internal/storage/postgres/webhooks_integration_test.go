package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
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

// TestWebhookRepoCRUD walks the Admin surface's round trip on a real database: what the create
// defaults, what a partial update leaves alone, and what the delete makes the list forget.
func TestWebhookRepoCRUD(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewWebhookRepo(pool)
	accountID := seedWebhookAccount(t, pool, "CrudCo", "crud-app")

	created, err := repo.Create(ctx, cp.NewWebhook{
		AccountID: accountID, EventType: cp.WebhookEventMO,
		URL: "https://acme.test/mo", Secret: "a-signing-secret-long-enough",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Status != cp.WebhookActive {
		t.Errorf("status = %q, want the DDL default active", created.Status)
	}
	if string(created.RetryPolicyJSON) != "{}" {
		t.Errorf("retry_policy_json = %q, want the empty object the query defaults to", created.RetryPolicyJSON)
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Errorf("timestamps = %v / %v, want both set", created.CreatedAt, created.UpdatedAt)
	}

	// A second webhook for an event type the account already subscribes to is webhooks_uq, and the
	// repository must turn it into a conflict rather than let a driver error reach the handler as a 500.
	if _, err := repo.Create(ctx, cp.NewWebhook{
		AccountID: accountID, EventType: cp.WebhookEventMO,
		URL: "https://acme.test/other", Secret: "another-secret-long-enough",
	}); !errors.Is(err, errs.ErrConflict) {
		t.Errorf("duplicate (account, event_type) = %v, want ErrConflict", err)
	}

	status := cp.WebhookDisabled
	updated, err := repo.Update(ctx, accountID, created.ID, cp.WebhookPatch{Status: &status})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Status != cp.WebhookDisabled {
		t.Errorf("status = %q, want disabled", updated.Status)
	}
	if updated.URL != created.URL || updated.Secret != created.Secret {
		t.Errorf("a status-only patch changed url or secret: %+v", updated)
	}

	// Disabled or not, the administration surface still lists it — that is how it gets switched back on.
	list, err := repo.List(ctx, accountID)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].ID != created.ID {
		t.Fatalf("List = %+v, want the one disabled webhook", list)
	}

	if err := repo.Delete(ctx, accountID, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := repo.Delete(ctx, accountID, created.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("second Delete = %v, want ErrNotFound", err)
	}
	if list, err := repo.List(ctx, accountID); err != nil || len(list) != 0 {
		t.Errorf("List after delete = %+v (err %v), want empty", list, err)
	}
}

// TestWebhookRepoIsScopedToItsAccount is the guard that keeps a webhook id from being a key to another
// account's row. The id is a UUID on a path whose account segment the caller also chooses, so account
// and id are ONE key — a read or write that trusted the id alone would let an operator scoped to one
// account read, rewrite or delete another's delivery URL, secret included.
func TestWebhookRepoIsScopedToItsAccount(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewWebhookRepo(pool)

	mine := seedWebhookAccount(t, pool, "MineCo", "mine-app")
	theirs := seedWebhookAccount(t, pool, "TheirsCo", "theirs-app")

	wh, err := repo.Create(ctx, cp.NewWebhook{
		AccountID: theirs, EventType: cp.WebhookEventMO,
		URL: "https://theirs.test/mo", Secret: "their-secret-long-enough",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if _, err := repo.Get(ctx, mine, wh.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("Get across accounts = %v, want ErrNotFound", err)
	}
	stolen := "https://attacker.test/mo"
	if _, err := repo.Update(ctx, mine, wh.ID, cp.WebhookPatch{URL: &stolen}); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("Update across accounts = %v, want ErrNotFound", err)
	}
	if err := repo.Delete(ctx, mine, wh.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("Delete across accounts = %v, want ErrNotFound", err)
	}

	// The row is untouched, not merely unreported: a failed scope check that still wrote would leave
	// the other account's traffic pointing at the attacker's URL while answering 404.
	after, err := repo.Get(ctx, theirs, wh.ID)
	if err != nil {
		t.Fatalf("Get(theirs): %v", err)
	}
	if after.URL != "https://theirs.test/mo" {
		t.Errorf("url = %q, want the owner's own url", after.URL)
	}
}

func seedWebhookAccount(t *testing.T, pool *pgxpool.Pool, customerName, accountName string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: customerName})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := postgres.NewAccountRepo(pool).Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: accountName})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	return account.ID
}
