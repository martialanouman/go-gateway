package postgres_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestSenderRewriteRulesListInEvaluationOrder: scope precedence first, priority second. The priorities
// run against the precedence (the platform rule is the lowest, the account rule outranks the customer
// one) and no two scopes sort alphabetically in precedence order, so a sort on priority, on the scope
// name, or with two ranks swapped all put a rule in the wrong place.
func TestSenderRewriteRulesListInEvaluationOrder(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "DELETE FROM control_plane.sender_id_rewrite_rules"); err != nil {
		t.Fatalf("clean: %v", err)
	}
	repo := postgres.NewSenderRewriteRuleRepo(pool)
	connectorID, accountID, customerID := uuid.New(), uuid.New(), uuid.New()

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
	account := create(cp.RewriteScopeAccount, &accountID, 90)
	connectorLate := create(cp.RewriteScopeConnector, &connectorID, 200)
	connectorEarly := create(cp.RewriteScopeConnector, &connectorID, 10)

	got, err := repo.List(ctx, cp.SenderRewriteFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []uuid.UUID{connectorEarly.ID, connectorLate.ID, account.ID, customer.ID, platform.ID}
	if len(got) != len(want) {
		t.Fatalf("got %d rules, want %d", len(got), len(want))
	}
	for i, r := range got {
		if r.ID != want[i] {
			t.Fatalf("position %d: %s/%d, want the rule %s", i, r.Scope, r.Priority, want[i])
		}
	}

	connector := cp.RewriteScopeConnector
	for name, tc := range map[string]struct {
		f    cp.SenderRewriteFilter
		want int
	}{
		"scope":    {cp.SenderRewriteFilter{Scope: &connector}, 2},
		"scope_id": {cp.SenderRewriteFilter{ScopeID: &customerID}, 1},
		"both":     {cp.SenderRewriteFilter{Scope: &connector, ScopeID: &customerID}, 0},
	} {
		if rules, err := repo.List(ctx, tc.f); err != nil || len(rules) != tc.want {
			t.Errorf("filter %s: %d rules, %v; want %d", name, len(rules), err, tc.want)
		}
	}
}

// sameRule compares every stored field; the server-assigned id and timestamps are checked apart.
func sameRule(t *testing.T, what string, got, want cp.SenderRewriteRule) {
	t.Helper()
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Errorf("%s: timestamps not read back", what)
	}
	got.ID, want.ID = uuid.Nil, uuid.Nil
	got.CreatedAt, got.UpdatedAt = time.Time{}, time.Time{}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s:\n got  %+v\n want %+v", what, got, want)
	}
}

func TestSenderRewriteRuleCRUDRoundTrip(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	repo := postgres.NewSenderRewriteRuleRepo(pool)
	accountID := uuid.New()

	r, err := repo.Create(ctx, cp.NewSenderRewriteRule{
		Scope: cp.RewriteScopeAccount, ScopeID: &accountID,
		MatchSenderPattern: ptr("[0-9]+"), MatchDestPattern: ptr("225.*"),
		RewriteType: cp.RewriteFallbackPool, FallbackPool: []string{"A", "B"}, MaxLength: ptr[int32](11),
		SanitizeCharset: []byte(`{"allowed":"AB"}`), Priority: 7, Reason: ptr("SMSC refuses numerics"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	want := cp.SenderRewriteRule{
		Scope: cp.RewriteScopeAccount, ScopeID: &accountID, Direction: "mt",
		MatchSenderPattern: ptr("[0-9]+"), MatchDestPattern: ptr("225.*"),
		RewriteType: cp.RewriteFallbackPool, FallbackPool: []string{"A", "B"}, MaxLength: ptr[int32](11),
		SanitizeCharset: []byte(`{"allowed": "AB"}`), Priority: 7, Reason: ptr("SMSC refuses numerics"),
		Status: "active",
	}
	sameRule(t, "created", r, want)

	static := cp.RewriteStatic
	disabled := "disabled"
	u, err := repo.Update(ctx, r.ID, cp.SenderRewriteRulePatch{
		MatchSenderPattern: ptr("[A-Z]+"), MatchDestPattern: ptr("33.*"),
		RewriteType: &static, RewriteTo: ptr("INFO"), FallbackPool: []string{"C"}, MaxLength: ptr[int32](9),
		SanitizeCharset: []byte(`{"allowed":"C"}`), Priority: ptr[int32](3), Reason: ptr("new carrier rule"),
		Status: &disabled,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	want = cp.SenderRewriteRule{
		Scope: cp.RewriteScopeAccount, ScopeID: &accountID, Direction: "mt",
		MatchSenderPattern: ptr("[A-Z]+"), MatchDestPattern: ptr("33.*"),
		RewriteType: cp.RewriteStatic, RewriteTo: ptr("INFO"), FallbackPool: []string{"C"}, MaxLength: ptr[int32](9),
		SanitizeCharset: []byte(`{"allowed": "C"}`), Priority: 3, Reason: ptr("new carrier rule"),
		Status: disabled,
	}
	sameRule(t, "updated", u, want)

	kept, err := repo.Update(ctx, r.ID, cp.SenderRewriteRulePatch{})
	if err != nil {
		t.Fatalf("empty update: %v", err)
	}
	sameRule(t, "after an empty patch", kept, want)
	got, err := repo.Get(ctx, r.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	sameRule(t, "read back", got, want)

	if err := repo.Delete(ctx, r.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := repo.Get(ctx, r.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
	if err := repo.Delete(ctx, r.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
}

func ptr[T any](v T) *T { return &v }
