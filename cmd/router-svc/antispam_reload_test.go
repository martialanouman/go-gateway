package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestAnAntispamRuleCreatedAfterBootReachesTheRunningRouter: an operator who creates a category_mismatch
// rule from the dashboard expects it on the next message, not at the router's next restart (step-291).
func TestAnAntispamRuleCreatedAfterBootReachesTheRunningRouter(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	pool := pgtest.Pool(t)
	customer := uuid.New()
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	app, err := newRouterApp(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("newRouterApp: %v", err)
	}
	watcherDone := make(chan error, 1)
	go func() { watcherDone <- app.watcher.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-watcherDone
		app.close()
	})

	evaluate := func() cp.AntispamAction {
		action, _ := app.antispam.Evaluate(ctx, uuid.New(), uuid.New(), customer, "BANK", cp.TrafficOTP, "2250700000001", []byte("no code"))
		return action
	}
	if got := evaluate(); got != "" {
		t.Fatalf("before the rule, an OTP without a code = %q, want nothing — the control failed", got)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO control_plane.antispam_rules (rule_type, scope, scope_id, config_json, action)
		VALUES ('category_mismatch', 'customer', $1, '{}', 'flag')`, customer); err != nil {
		t.Fatalf("create the rule: %v", err)
	}
	pub := redisstore.NewPubSubPublisher(redistest.Client(t))
	deadline := time.Now().Add(15 * time.Second)
	for evaluate() != cp.AntispamActionFlag {
		if err := pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`)); err != nil {
			t.Fatalf("publish invalidation: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the running router never applied the category_mismatch rule created after its boot")
		}
		time.Sleep(300 * time.Millisecond)
	}
}
