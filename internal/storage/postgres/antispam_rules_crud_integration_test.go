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

// TestAntispamRuleAdminCRUD exercises the Admin write/read surface against real Postgres: a global
// create, an account-scoped create, the antispam_scope_ck CHECK, list (active + disabled), update,
// and delete-by-id.
func TestAntispamRuleAdminCRUD(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()
	// antispam_rules is a global table read unfiltered; start from a clean slate so shared-pool tests
	// do not see each other's rows.
	if _, err := pool.Exec(ctx, "DELETE FROM control_plane.antispam_rules"); err != nil {
		t.Fatalf("clean antispam_rules: %v", err)
	}
	repo := postgres.NewAntispamRuleRepo(pool)

	// A global rule (no scope_id).
	global, err := repo.Create(ctx, cp.NewAntispamRule{
		RuleType: cp.AntispamContentBlacklist, Scope: cp.AntispamScopeGlobal,
		ConfigJSON: []byte(`{"patterns":["(?i)spam"]}`), Action: cp.AntispamActionBlock,
	})
	if err != nil {
		t.Fatalf("create global: %v", err)
	}
	if global.Scope != cp.AntispamScopeGlobal || global.ScopeID != nil || global.Status != cp.AntispamRuleActive {
		t.Errorf("global rule = %+v, want global/nil/active", global)
	}

	// An account-scoped rule (scope_id required).
	acct := uuid.New()
	if _, err := repo.Create(ctx, cp.NewAntispamRule{
		RuleType: cp.AntispamVelocity, Scope: cp.AntispamScopeAccount, ScopeID: &acct,
		ConfigJSON: []byte(`{"max":100,"window_seconds":60,"by":"source"}`), Action: cp.AntispamActionThrottle,
	}); err != nil {
		t.Fatalf("create account-scoped: %v", err)
	}

	// The antispam_scope_ck CHECK: a global rule with a scope_id is rejected as a validation error.
	if _, err := repo.Create(ctx, cp.NewAntispamRule{
		RuleType: cp.AntispamDuplicate, Scope: cp.AntispamScopeGlobal, ScopeID: &acct,
		ConfigJSON: []byte(`{"window_seconds":60}`), Action: cp.AntispamActionFlag,
	}); !errors.Is(err, errs.ErrValidation) {
		t.Errorf("global+scope_id create = %v, want ErrValidation (antispam_scope_ck)", err)
	}

	// List returns both rules; ListActive too (both are active).
	all, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("List returned %d, want 2", len(all))
	}

	// Disable the global rule → excluded from ListActive, present in List.
	disabled := cp.AntispamRuleDisabled
	if _, err := repo.Update(ctx, global.ID, cp.AntispamRulePatch{Status: &disabled}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	active, _ := repo.ListActive(ctx)
	if len(active) != 1 {
		t.Errorf("ListActive returned %d, want 1 (global disabled)", len(active))
	}

	// Delete by id, then a second delete is ErrNotFound.
	if err := repo.Delete(ctx, global.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := repo.Delete(ctx, global.ID); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("second delete = %v, want ErrNotFound", err)
	}
}
