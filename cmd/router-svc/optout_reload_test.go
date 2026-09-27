package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestAStopAnnouncedOnItsOwnChannelReachesTheRunningRouter: a STOP received as an MO writes its
// suppression and announces it on optout:changed ONLY — no config:changed, no snapshot invalidation. The
// router's opt-out filter answers "not suppressed" without reading the database, so before step-398 this
// recipient kept receiving MTs until some unrelated admin mutation rebuilt the filter (§6.20).
func TestAStopAnnouncedOnItsOwnChannelReachesTheRunningRouter(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	app, err := newRouterApp(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("newRouterApp: %v", err)
	}
	watchersDone := make(chan struct{}, 2)
	for _, w := range []interface{ Run(context.Context) error }{app.watcher, app.optOutWatcher} {
		go func() { _ = w.Run(ctx); watchersDone <- struct{}{} }()
	}
	t.Cleanup(func() {
		cancel()
		<-watchersDone
		<-watchersDone
		app.close()
	})

	// Per-run number: the shared database keeps every sibling run's suppressions.
	recipient := fmt.Sprintf("22507%08d", rand.IntN(100_000_000))
	optedOut := func() bool {
		t.Helper()
		blocked, err := app.optOut.IsOptedOut(ctx, uuid.New(), uuid.New(), "ACME", recipient)
		if err != nil {
			t.Fatalf("IsOptedOut: %v", err)
		}
		return blocked
	}
	if optedOut() {
		t.Fatal("the recipient is opted out before any STOP — the control failed")
	}

	if _, err := postgres.NewSuppressionRepo(pgtest.Pool(t)).Create(ctx, cp.NewSuppression{
		Scope: cp.SuppressionScopePlatform, MSISDN: recipient, Source: cp.SuppressionSourceMOStop,
	}); err != nil {
		t.Fatalf("write the STOP suppression: %v", err)
	}
	pub := redisstore.NewPubSubPublisher(redistest.Client(t))
	deadline := time.Now().Add(15 * time.Second)
	for !optedOut() {
		// Republished until seen: the watcher may not have subscribed yet when the first one goes out.
		if err := pub.Publish(ctx, config.ChannelOptOutChanged, []byte(`{"reason":"optout"}`)); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the running router still sends to a recipient who sent STOP: optout:changed reached " +
				"no opt-out reload")
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// TestAStopWhoseAnnouncementIsLostReachesTheRouterOnResync: the STOP is written and NOTHING is published —
// the announcement lost to a Redis blip. Only the snapshot watcher runs, not the opt-out one: its periodic
// resync reloads the opt-out filter too, and that is what bounds the send to a recipient who opted out
// (step-399).
func TestAStopWhoseAnnouncementIsLostReachesTheRouterOnResync(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.ConfigResyncInterval = 500 * time.Millisecond
	app, err := newRouterApp(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("newRouterApp: %v", err)
	}
	watcherDone := make(chan struct{})
	go func() { _ = app.watcher.Run(ctx); close(watcherDone) }()
	t.Cleanup(func() {
		cancel()
		<-watcherDone
		app.close()
	})

	recipient := fmt.Sprintf("22507%08d", rand.IntN(100_000_000))
	optedOut := func() bool {
		t.Helper()
		blocked, err := app.optOut.IsOptedOut(ctx, uuid.New(), uuid.New(), "ACME", recipient)
		if err != nil {
			t.Fatalf("IsOptedOut: %v", err)
		}
		return blocked
	}
	if optedOut() {
		t.Fatal("the recipient is opted out before any STOP — the control failed")
	}

	if _, err := postgres.NewSuppressionRepo(pgtest.Pool(t)).Create(ctx, cp.NewSuppression{
		Scope: cp.SuppressionScopePlatform, MSISDN: recipient, Source: cp.SuppressionSourceMOStop,
	}); err != nil {
		t.Fatalf("write the STOP suppression: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !optedOut() {
		if time.Now().After(deadline) {
			t.Fatal("the running router still sends to a recipient who sent STOP: with no announcement, " +
				"nothing ever rebuilt its opt-out filter")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
