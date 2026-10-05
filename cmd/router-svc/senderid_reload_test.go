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

// TestADisabledSenderIDReachesTheRunningRouter: registrations are checked per message, so a router that
// refuses after the change refuses on every session already open. Before step-390 the snapshot was loaded
// at boot only and a PATCH was true in the database and false in production.
func TestADisabledSenderIDReachesTheRunningRouter(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	pool := pgtest.Pool(t)
	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "390-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO control_plane.sender_ids (customer_id, address, status) VALUES ($1, 'PROMO', 'active')`, customer.ID); err != nil {
		t.Fatalf("register sender: %v", err)
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

	authorize := func() error { return app.senderIDs.Authorize(ctx, customer.ID, "PROMO") }
	if err := authorize(); err != nil {
		t.Fatalf("an active registered sender = %v, want accepted — the control failed", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE control_plane.sender_ids SET status = 'disabled' WHERE customer_id = $1`, customer.ID); err != nil {
		t.Fatalf("disable the sender: %v", err)
	}
	pub := redisstore.NewPubSubPublisher(redistest.Client(t))
	deadline := time.Now().Add(15 * time.Second)
	for !errors.Is(authorize(), errs.ErrSenderIDNotAuthorized) {
		if err := pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`)); err != nil {
			t.Fatalf("publish invalidation: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the running router still accepts the sender after it was disabled")
		}
		time.Sleep(300 * time.Millisecond)
	}
}
