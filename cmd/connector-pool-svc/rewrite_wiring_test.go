package main

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderrewrite"
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

	clean := func() {
		if _, err := pool.Exec(context.Background(), "DELETE FROM control_plane.sender_id_rewrite_rules WHERE true"); err != nil {
			t.Fatalf("clean: %v", err)
		}
	}
	clean()
	t.Cleanup(clean) // a platform rule left behind would rewrite every sender of the next pool test
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
	// The pool must send through that very holder: nothing else shows a missing Rewriter in the wiring,
	// which would only rewrite nothing.
	deps := reflect.ValueOf(app.pool).Elem().FieldByName("deps")
	wired := deps.FieldByName("Rewriter")
	if !wired.IsValid() || wired.IsNil() || wired.Elem().Pointer() != reflect.ValueOf(app.rewriter).Pointer() {
		t.Error("the pool is not wired to the holder the watcher swaps")
	}
	// Without the Redis pins a rerouted segment would go out under the fallback connector's rules.
	if pins := deps.FieldByName("SenderPins"); !pins.IsValid() || pins.IsNil() || pins.Elem().Type().String() != "*dlrmap.SenderPins" {
		t.Error("the pool does not pin multipart senders in Redis")
	}

	watchCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); _ = app.rewriteWatcher.Run(watchCtx) }()
	defer func() { stop(); <-done }()
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

type failingLister struct{}

func (failingLister) List(context.Context, cp.SenderRewriteFilter) ([]cp.SenderRewriteRule, error) {
	return nil, errors.New("postgres down")
}

// A reload that cannot read the rules reports it and keeps the ones in force: a Postgres blip must not
// silently switch every rewrite off.
func TestLoadRewritesKeepsTheRulesOnAReadFailure(t *testing.T) {
	var h senderrewrite.Holder
	to := "INFO"
	h.Store(senderrewrite.Build([]cp.SenderRewriteRule{{
		Scope: cp.RewriteScopePlatform, Direction: "mt", Status: "active", RewriteType: cp.RewriteStatic, RewriteTo: &to,
	}}, nil))
	if err := loadRewrites(t.Context(), failingLister{}, &h, silentLogger()); err == nil {
		t.Fatal("a failed read reported no error")
	}
	if got := h.Rewrite(uuid.New(), uuid.New(), uuid.New(), "ACME", "225", uuid.New()); got != "INFO" {
		t.Fatalf("after a failed reload: %q, want the rule in force", got)
	}
}

// roleDeniedOn is a login role that reads every control-plane table but one, and the database URL that
// connects as it. It fails one boot read and nothing else — which a closed port or a bad URL cannot do,
// since openStores would fail first.
func roleDeniedOn(t *testing.T, table string) string {
	t.Helper()
	pool := pgtest.Pool(t)
	role := "pool_boot_" + uuid.NewString()[:8]
	for _, stmt := range []string{
		"CREATE ROLE " + role + " LOGIN PASSWORD 'pw'",
		"GRANT USAGE ON SCHEMA control_plane TO " + role,
		"GRANT SELECT ON ALL TABLES IN SCHEMA control_plane TO " + role,
		"REVOKE SELECT ON control_plane." + table + " FROM " + role,
	} {
		if _, err := pool.Exec(t.Context(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP OWNED BY "+role)
		_, _ = pool.Exec(context.Background(), "DROP ROLE "+role)
	})
	u, err := url.Parse(pgtest.Config(t).URL)
	if err != nil {
		t.Fatalf("parse the test database url: %v", err)
	}
	u.User = url.UserPassword(role, "pw")
	return u.String()
}

// A pod that cannot read one of the snapshots it enforces must not serve without it: the rewrite rules
// (the SMSC refuses the formats they fix) and the reroute rate limits (the fallback connector's ceiling).
func TestNewPoolAppRefusesToBootWithoutItsSnapshots(t *testing.T) {
	for table, want := range map[string]string{
		"sender_id_rewrite_rules": "sender rewrite rules",
		"rate_limits":             "rate-limit snapshot",
	} {
		t.Run(table, func(t *testing.T) {
			cfg := testConfig()
			cfg.Postgres = pgtest.Config(t)
			cfg.Postgres.URL = roleDeniedOn(t, table)
			cfg.Redis = redistest.Config(t)

			app, err := newPoolApp(t.Context(), cfg, testBindEnv(), silentLogger())
			if err == nil {
				app.close()
				t.Fatalf("booted without reading %s", table)
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name the %s", err, want)
			}
		})
	}
}
