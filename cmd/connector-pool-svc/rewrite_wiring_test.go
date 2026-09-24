package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestNewPoolAppRewritesWithTheRulesAndFollowsAnInvalidation: the booted pool loads the rules before it
// serves, and a config-sync invalidation swaps in the new set without a restart.
func TestNewPoolAppRewritesWithTheRulesAndFollowsAnInvalidation(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	pool, rdb := pgtest.Pool(t), redistest.Client(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, "DELETE FROM control_plane.sender_id_rewrite_rules WHERE true"); err != nil {
		t.Fatalf("clean: %v", err)
	}
	repo := postgres.NewSenderRewriteRuleRepo(pool)
	rule := func(scope cp.SenderRewriteScope, scopeID *uuid.UUID, to string) {
		t.Helper()
		if _, err := repo.Create(ctx, cp.NewSenderRewriteRule{
			Scope: scope, ScopeID: scopeID, RewriteType: cp.RewriteStatic, RewriteTo: &to, Priority: 1,
		}); err != nil {
			t.Fatalf("create rule: %v", err)
		}
	}
	rule(cp.RewriteScopePlatform, nil, "BOOT")

	env := testBindEnv()
	app, err := newPoolApp(ctx, cfg, env, silentLogger())
	if err != nil {
		t.Fatalf("newPoolApp: %v", err)
	}
	defer app.close()

	sent := func() string {
		return app.rewriter.Rewrite(env.ID, uuid.New(), uuid.New(), "ACME", "2250700000001", uuid.New())
	}
	if got := sent(); got != "BOOT" {
		t.Fatalf("after boot: %q, want BOOT", got)
	}

	go func() { _ = app.rewriteWatcher.Run(ctx) }()
	rule(cp.RewriteScopeConnector, &env.ID, "RELOADED")
	for deadline := time.Now().Add(10 * time.Second); sent() != "RELOADED"; {
		if time.Now().After(deadline) {
			t.Fatalf("after an invalidation: %q, want RELOADED", sent())
		}
		// Published until seen: the watcher may not have subscribed yet when the first one goes out.
		_ = rdb.Publish(ctx, config.ChannelSnapshotInvalidation, "{}").Err()
		time.Sleep(100 * time.Millisecond)
	}
}
