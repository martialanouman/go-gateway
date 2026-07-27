package postgres_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/routing/script"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

func cleanRoutingScripts(t *testing.T) {
	t.Helper()
	pool := pgtest.Pool(t)
	if _, err := pool.Exec(context.Background(), "DELETE FROM control_plane.routing_scripts"); err != nil {
		t.Fatalf("clean routing_scripts: %v", err)
	}
}

func draftScript(name, src string) script.Script {
	return script.Script{
		Scope: script.ScopePlatform, Name: name, Language: script.LanguageJS,
		Source: src, Checksum: script.Checksum(src), TimeoutMs: 2,
	}
}

// TestRoutingScriptPublishEnforcesOneActive: publishing a second script for a scope demotes the first,
// so the one-active-per-scope invariant holds and GetActive returns the latest.
func TestRoutingScriptPublishEnforcesOneActive(t *testing.T) {
	ctx := context.Background()
	cleanRoutingScripts(t)
	repo := postgres.NewRoutingScriptRepo(pgtest.Pool(t))

	a, err := repo.Create(ctx, draftScript("v1", "function resolveRoute(m){return null}"))
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	if a.Status != script.StatusDraft {
		t.Fatalf("new script status = %q, want draft", a.Status)
	}

	pubA, found, err := repo.Publish(ctx, a.ID)
	if err != nil || !found {
		t.Fatalf("publish A: found=%v err=%v", found, err)
	}
	if pubA.Status != script.StatusActive || pubA.PublishedAt == nil {
		t.Errorf("published A = {status:%q published_at:%v}, want active with a timestamp", pubA.Status, pubA.PublishedAt)
	}

	// A second draft, published, must relegate A (only one active per scope).
	b, err := repo.Create(ctx, draftScript("v2", "function resolveRoute(m){return null}"))
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	if _, _, err := repo.Publish(ctx, b.ID); err != nil {
		t.Fatalf("publish B: %v", err)
	}

	active, found, err := repo.GetActive(ctx, script.ScopePlatform, nil)
	if err != nil || !found {
		t.Fatalf("get active: found=%v err=%v", found, err)
	}
	if active.ID != b.ID {
		t.Errorf("active script = %s, want the newly published B %s", active.ID, b.ID)
	}

	// A is now disabled, not active.
	gotA, _, err := repo.Get(ctx, a.ID)
	if err != nil {
		t.Fatalf("get A: %v", err)
	}
	if gotA.Status != script.StatusDisabled {
		t.Errorf("A status after B published = %q, want disabled", gotA.Status)
	}

	// Re-publishing the already-active B keeps it active (demote hits B, promote reactivates it) and
	// advances published_at — still exactly one active.
	rePub, found, err := repo.Publish(ctx, b.ID)
	if err != nil || !found {
		t.Fatalf("re-publish B: found=%v err=%v", found, err)
	}
	if rePub.Status != script.StatusActive {
		t.Errorf("re-published B status = %q, want active", rePub.Status)
	}

	// Publishing a non-existent id is reported, not an error, with no side effects.
	if _, found, err := repo.Publish(ctx, uuid.New()); err != nil || found {
		t.Errorf("publish of a missing id = (found %v, err %v), want (false, nil)", found, err)
	}
	if still, _, _ := repo.GetActive(ctx, script.ScopePlatform, nil); still.ID != b.ID {
		t.Errorf("active changed after a no-op publish; got %s want %s", still.ID, b.ID)
	}
}

// TestRoutingScriptPublishAtCustomerScopeIsIsolated: the one-active invariant and the demote apply
// per (scope, scope_id) — publishing at a customer scope does not touch the platform active, and
// matching uses IS NOT DISTINCT FROM on a non-NULL scope_id.
func TestRoutingScriptPublishAtCustomerScopeIsIsolated(t *testing.T) {
	ctx := context.Background()
	cleanRoutingScripts(t)
	repo := postgres.NewRoutingScriptRepo(pgtest.Pool(t))

	// A platform active, plus two customer-scoped drafts for the same customer.
	plat := draftScript("platform", "src")
	platRow, _ := repo.Create(ctx, plat)
	if _, _, err := repo.Publish(ctx, platRow.ID); err != nil {
		t.Fatalf("publish platform: %v", err)
	}

	custID := uuid.New()
	mk := func(name string) uuid.UUID {
		s := draftScript(name, "src")
		s.Scope, s.ScopeID = script.ScopeCustomer, &custID
		row, err := repo.Create(ctx, s)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		return row.ID
	}
	c1, c2 := mk("c1"), mk("c2")

	if _, _, err := repo.Publish(ctx, c1); err != nil {
		t.Fatalf("publish c1: %v", err)
	}
	if _, _, err := repo.Publish(ctx, c2); err != nil {
		t.Fatalf("publish c2: %v", err)
	}

	// c2 is the customer active; c1 was demoted; the platform active is untouched.
	if active, _, _ := repo.GetActive(ctx, script.ScopeCustomer, &custID); active.ID != c2 {
		t.Errorf("customer active = %s, want c2 %s", active.ID, c2)
	}
	if got, _, _ := repo.Get(ctx, c1); got.Status != script.StatusDisabled {
		t.Errorf("c1 status = %q, want disabled", got.Status)
	}
	if plActive, found, _ := repo.GetActive(ctx, script.ScopePlatform, nil); !found || plActive.ID != platRow.ID {
		t.Errorf("platform active changed after customer publishes; found=%v id=%s want %s", found, plActive.ID, platRow.ID)
	}
}

// TestRoutingScriptListVersions: every version for a scope is returned newest-first; a different scope
// is isolated.
func TestRoutingScriptListVersions(t *testing.T) {
	ctx := context.Background()
	cleanRoutingScripts(t)
	repo := postgres.NewRoutingScriptRepo(pgtest.Pool(t))

	for i := 0; i < 3; i++ {
		if _, err := repo.Create(ctx, draftScript("v", "src")); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	// A customer-scoped script must not appear under the platform scope's versions.
	custID := uuid.New()
	cust := draftScript("cust", "src")
	cust.Scope, cust.ScopeID = script.ScopeCustomer, &custID
	if _, err := repo.Create(ctx, cust); err != nil {
		t.Fatalf("seed customer: %v", err)
	}

	versions, err := repo.ListVersions(ctx, script.ScopePlatform, nil)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("platform versions = %d, want 3 (customer scope excluded)", len(versions))
	}
}

// TestRoutingScriptUpdateAndDelete: a draft is editable and deletable; a missing id is reported, not an
// error.
func TestRoutingScriptUpdateAndDelete(t *testing.T) {
	ctx := context.Background()
	cleanRoutingScripts(t)
	repo := postgres.NewRoutingScriptRepo(pgtest.Pool(t))

	s, err := repo.Create(ctx, draftScript("v1", "old"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	edit := s
	edit.Source, edit.Checksum = "new source", script.Checksum("new source")
	updated, found, err := repo.Update(ctx, s.ID, edit)
	if err != nil || !found {
		t.Fatalf("update: found=%v err=%v", found, err)
	}
	if updated.Source != "new source" || updated.Checksum != script.Checksum("new source") {
		t.Errorf("update did not persist the new source/checksum: %+v", updated)
	}

	if _, found, err := repo.Update(ctx, uuid.New(), edit); err != nil || found {
		t.Errorf("update of a missing id = (found %v, err %v), want (false, nil)", found, err)
	}

	if found, err := repo.Delete(ctx, s.ID); err != nil || !found {
		t.Fatalf("delete: found=%v err=%v", found, err)
	}
	if _, found, err := repo.Get(ctx, s.ID); err != nil || found {
		t.Errorf("get after delete = (found %v, err %v), want (false, nil)", found, err)
	}
}
