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

// TestWebhookRepoGet proves Get returns a configured webhook and reports a clean absence (found=false)
// for an event type the account has not subscribed to. It also pins the property both delivery paths
// depend on: a DISABLED webhook is returned, not hidden. Filtering it here would look tidier and would
// take from the deferred retry runner the one thing it needs to tell "switched off" from "deleted".
func TestWebhookRepoGet(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewWebhookRepo(pool)
	accountID := seedWebhookAccount(t, pool, "WebhookCo", "webhook-app")

	seeded, err := repo.Create(ctx, cp.NewWebhook{
		AccountID: accountID, EventType: cp.WebhookEventMO,
		URL: "https://example.test/hook", Secret: "topsecret-long-enough",
		RetryPolicyJSON: []byte(`{"max_attempts":4}`),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, found, err := repo.Get(ctx, accountID, cp.WebhookEventMO)
	if err != nil {
		t.Fatalf("Get(mo): %v", err)
	}
	if !found {
		t.Fatal("Get(mo) found=false, want the seeded webhook")
	}
	if got.URL != "https://example.test/hook" || got.Secret != "topsecret-long-enough" || got.Status != cp.WebhookActive {
		t.Errorf("webhook = %+v, want the seeded url/secret/active", got)
	}
	if string(got.RetryPolicyJSON) != `{"max_attempts": 4}` {
		t.Errorf("retry_policy_json = %s, want the seeded policy", got.RetryPolicyJSON)
	}

	if _, found, err := repo.Get(ctx, accountID, cp.WebhookEventDLR); err != nil || found {
		t.Errorf("Get(dlr) = found %v err %v, want no webhook (found=false, nil err)", found, err)
	}

	disabled := cp.WebhookDisabled
	if _, err := repo.Update(ctx, accountID, seeded.ID, cp.WebhookPatch{Status: &disabled}); err != nil {
		t.Fatalf("Update(disabled): %v", err)
	}
	off, found, err := repo.Get(ctx, accountID, cp.WebhookEventMO)
	if err != nil || !found {
		t.Fatalf("Get after disabling = found %v err %v, want the row", found, err)
	}
	if off.Status != cp.WebhookDisabled {
		t.Errorf("status = %q, want disabled — the caller cannot decide what it cannot see", off.Status)
	}
}

// TestWebhookRepoCRUD walks the Admin surface's round trip on a real database: what the create
// defaults, what a partial update writes and leaves alone, and what the delete makes the list forget.
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

	// Every writable field at once, so a url/secret swap in the parameter mapping cannot hide behind a
	// patch that only ever carries one of them.
	url, secret, status := "https://acme.test/mo/v2", "a-rotated-secret-long-enough", cp.WebhookDisabled
	updated, err := repo.Update(ctx, accountID, created.ID, cp.WebhookPatch{
		URL: &url, Secret: &secret, Status: &status, RetryPolicyJSON: []byte(`{"max_attempts":9}`),
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.URL != url || updated.Secret != secret || updated.Status != cp.WebhookDisabled {
		t.Errorf("updated = %+v, want the patched url, secret and status", updated)
	}
	if string(updated.RetryPolicyJSON) != `{"max_attempts": 9}` {
		t.Errorf("retry_policy_json = %s, want the patched policy", updated.RetryPolicyJSON)
	}
	if !updated.UpdatedAt.After(created.UpdatedAt) {
		t.Errorf("updated_at = %v, not after %v — the webhooks_touch trigger did not fire",
			updated.UpdatedAt, created.UpdatedAt)
	}

	// A patch that carries nothing must leave every column alone: that is what COALESCE is there for,
	// and it is the failure mode of a mapping that passes a zero value where it means "unchanged".
	untouched, err := repo.Update(ctx, accountID, created.ID, cp.WebhookPatch{})
	if err != nil {
		t.Fatalf("Update(empty): %v", err)
	}
	if untouched.URL != url || untouched.Secret != secret || untouched.Status != cp.WebhookDisabled ||
		string(untouched.RetryPolicyJSON) != `{"max_attempts": 9}` {
		t.Errorf("an empty patch changed something: %+v", untouched)
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
// and id are ONE key — a write that trusted the id alone would let an operator scoped to one account
// rewrite or delete another's delivery URL.
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

	stolen := "https://attacker.test/mo"
	if _, err := repo.Update(ctx, mine, wh.ID, cp.WebhookPatch{URL: &stolen}); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("Update across accounts = %v, want ErrNotFound", err)
	}
	if err := repo.Delete(ctx, mine, wh.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("Delete across accounts = %v, want ErrNotFound", err)
	}
	// List is asserted EMPTY rather than "not containing theirs": a WHERE dropped from the list query
	// would otherwise pass here on whatever rows this account happens not to own.
	if list, err := repo.List(ctx, mine); err != nil || len(list) != 0 {
		t.Errorf("List(mine) = %+v (err %v), want empty", list, err)
	}

	// The row is untouched, not merely unreported: a failed scope check that still wrote would leave
	// the other account's traffic pointing at the attacker's URL while answering 404.
	after, err := repo.List(ctx, theirs)
	if err != nil {
		t.Fatalf("List(theirs): %v", err)
	}
	if len(after) != 1 || after[0].URL != "https://theirs.test/mo" {
		t.Errorf("owner's webhooks = %+v, want the single untouched row", after)
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
