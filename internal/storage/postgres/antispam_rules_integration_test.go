package postgres_test

import (
	"context"
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestAntispamRuleRepoListActive proves the repo loads and maps active rules from real Postgres, and
// excludes disabled ones — the shape the router's boot snapshot consumes.
func TestAntispamRuleRepoListActive(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := context.Background()

	if _, err := pool.Exec(ctx,
		`INSERT INTO control_plane.antispam_rules (rule_type, scope, config_json, action, status) VALUES
		 ('content_blacklist', 'global', '{"patterns":["(?i)spam"]}', 'block', 'active'),
		 ('duplicate', 'global', '{"window_seconds":60}', 'throttle', 'active'),
		 ('content_blacklist', 'global', '{"patterns":["x"]}', 'flag', 'disabled')`); err != nil {
		t.Fatalf("seed rules: %v", err)
	}

	rules, err := postgres.NewAntispamRuleRepo(pool).ListActive(ctx)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("ListActive returned %d rules, want 2 active (1 disabled excluded)", len(rules))
	}

	var content, dup bool
	for _, r := range rules {
		if r.RuleType == cp.AntispamContentBlacklist && r.Action == cp.AntispamActionBlock {
			content = true
			if len(r.ConfigJSON) == 0 {
				t.Error("content rule must carry its config_json")
			}
		}
		if r.RuleType == cp.AntispamDuplicate && r.Action == cp.AntispamActionThrottle {
			dup = true
		}
	}
	if !content || !dup {
		t.Errorf("expected the content(block) and duplicate(throttle) rules; got content=%t dup=%t", content, dup)
	}
}
