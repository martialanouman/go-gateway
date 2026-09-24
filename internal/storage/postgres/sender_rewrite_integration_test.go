package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestSenderRewriteRulesListInEvaluationOrder: scope precedence first, priority second. The platform
// rule carries the lowest priority, so a sort on priority alone would put it first — plausible and wrong.
func TestSenderRewriteRulesListInEvaluationOrder(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "DELETE FROM control_plane.sender_id_rewrite_rules"); err != nil {
		t.Fatalf("clean: %v", err)
	}
	repo := postgres.NewSenderRewriteRuleRepo(pool)
	connectorID, customerID := uuid.New(), uuid.New()

	create := func(scope cp.SenderRewriteScope, scopeID *uuid.UUID, priority int32) cp.SenderRewriteRule {
		t.Helper()
		r, err := repo.Create(ctx, cp.NewSenderRewriteRule{
			Scope: scope, ScopeID: scopeID, RewriteType: cp.RewriteStatic, RewriteTo: ptr("INFO"), Priority: priority,
		})
		if err != nil {
			t.Fatalf("create %s: %v", scope, err)
		}
		return r
	}
	platform := create(cp.RewriteScopePlatform, nil, 1)
	customer := create(cp.RewriteScopeCustomer, &customerID, 50)
	connectorLate := create(cp.RewriteScopeConnector, &connectorID, 200)
	connectorEarly := create(cp.RewriteScopeConnector, &connectorID, 10)

	got, err := repo.List(ctx, cp.SenderRewriteFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []uuid.UUID{connectorEarly.ID, connectorLate.ID, customer.ID, platform.ID}
	if len(got) != len(want) {
		t.Fatalf("got %d rules, want %d", len(got), len(want))
	}
	for i, r := range got {
		if r.ID != want[i] {
			t.Fatalf("position %d: %s/%d, want the rule %s", i, r.Scope, r.Priority, want[i])
		}
	}

	scope := cp.RewriteScopeConnector
	filtered, err := repo.List(ctx, cp.SenderRewriteFilter{Scope: &scope, ScopeID: &connectorID})
	if err != nil || len(filtered) != 2 {
		t.Fatalf("filtered list = %d rules, %v; want the 2 connector rules", len(filtered), err)
	}
	other := uuid.New()
	if none, _ := repo.List(ctx, cp.SenderRewriteFilter{ScopeID: &other}); len(none) != 0 {
		t.Fatalf("scopeId filter ignored: %d rules", len(none))
	}
}

func TestSenderRewriteRuleCRUDRoundTrip(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewSenderRewriteRuleRepo(pool)
	accountID := uuid.New()

	r, err := repo.Create(ctx, cp.NewSenderRewriteRule{
		Scope: cp.RewriteScopeAccount, ScopeID: &accountID, MatchSenderPattern: ptr("[0-9]+"),
		RewriteType: cp.RewriteFallbackPool, FallbackPool: []string{"A", "B"},
		SanitizeCharset: []byte(`{"allowed":"AB"}`), Priority: 7, Reason: ptr("SMSC refuses numerics"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if r.Direction != "mt" || r.Status != "active" || len(r.FallbackPool) != 2 || r.FallbackPool[1] != "B" ||
		string(r.SanitizeCharset) != `{"allowed": "AB"}` || *r.MatchSenderPattern != "[0-9]+" {
		t.Fatalf("created = %+v", r)
	}

	newType := cp.RewriteStatic
	disabled := "disabled"
	u, err := repo.Update(ctx, r.ID, cp.SenderRewriteRulePatch{RewriteType: &newType, RewriteTo: ptr("INFO"), Status: &disabled})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if u.RewriteType != cp.RewriteStatic || *u.RewriteTo != "INFO" || u.Status != disabled || u.Priority != 7 ||
		len(u.FallbackPool) != 2 {
		t.Fatalf("updated = %+v; want the patched fields changed and the others kept", u)
	}

	if g, err := repo.Get(ctx, r.ID); err != nil || g.ID != r.ID {
		t.Fatalf("get = %v, %v", g.ID, err)
	}
	if err := repo.Delete(ctx, r.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := repo.Get(ctx, r.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
	if err := repo.Delete(ctx, r.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
	if _, err := repo.Update(ctx, r.ID, cp.SenderRewriteRulePatch{Status: &disabled}); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("update after delete = %v, want ErrNotFound", err)
	}
}

func ptr[T any](v T) *T { return &v }
