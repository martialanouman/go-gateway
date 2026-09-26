package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestAHardenedSenderIDPolicyReachesTheRunningRouter: the policy is checked per message, so a router that
// refuses after the change refuses on every session already open. Before step-390 the snapshot was loaded
// at boot only and a PATCH was true in the database and false in production.
func TestAHardenedSenderIDPolicyReachesTheRunningRouter(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	pool := pgtest.Pool(t)
	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "390-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	disabled := cp.SenderIDPolicyDisabled
	accounts := postgres.NewAccountRepo(pool)
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "390", SenderIDPolicy: &disabled})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

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

	authorize := func() error { return app.senderIDs.Authorize(ctx, account.ID, customer.ID, "PROMO") }
	if err := authorize(); err != nil {
		t.Fatalf("an unregistered sender under the disabled policy = %v, want accepted — the control failed", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE control_plane.smpp_accounts SET sender_id_policy = 'strict' WHERE id = $1`, account.ID); err != nil {
		t.Fatalf("harden the policy: %v", err)
	}
	pub := redisstore.NewPubSubPublisher(redistest.Client(t))
	deadline := time.Now().Add(15 * time.Second)
	for !errors.Is(authorize(), errs.ErrSenderIDNotAuthorized) {
		if err := pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`)); err != nil {
			t.Fatalf("publish invalidation: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the running router still accepts the sender after the policy became strict")
		}
		time.Sleep(300 * time.Millisecond)
	}
}
