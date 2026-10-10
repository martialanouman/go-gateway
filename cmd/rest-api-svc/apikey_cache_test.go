package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// step-287j: the running pod keeps a key's principal in memory, and forgets it on the config
// announcement that follows a revocation.
func TestARevokedKeyIsRefusedOnceTheRevocationIsAnnounced(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.Kafka.Brokers = kafkatest.Brokers(t)
	pool := pgtest.Pool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "287j-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := postgres.NewAccountRepo(pool).Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "287j"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	key, hash, err := credential.GenerateAPIKey()
	if err != nil {
		t.Fatalf("generate api key: %v", err)
	}
	creds := postgres.NewCredentialRepo(pool)
	cred, err := creds.Create(ctx, cp.NewCredential{AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &hash})
	if err != nil {
		t.Fatalf("create credential: %v", err)
	}

	app, err := newRestAPIApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRestAPIApp: %v", err)
	}
	watchCtx, stopWatch := context.WithCancel(ctx)
	watcherDone := make(chan error, 1)
	go func() { watcherDone <- app.watcher.Run(watchCtx) }()
	t.Cleanup(func() {
		stopWatch()
		<-watcherDone
		app.close()
	})
	api := httptest.NewServer(app.http.Handler)
	defer api.Close()

	getAccount := func() int {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, api.URL+"/v1/account", nil)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := api.Client().Do(req)
		if err != nil {
			t.Fatalf("get account: %v", err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	if got := getAccount(); got != http.StatusOK {
		t.Fatalf("GET /v1/account with a fresh key = %d, want 200", got)
	}
	if _, err := creds.SetStatus(ctx, account.ID, cred.ID, cp.CredentialRevoked); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if got := getAccount(); got != http.StatusOK {
		t.Fatalf("GET /v1/account right after an unannounced revocation = %d, want 200 from the cache", got)
	}

	pub := redisstore.NewPubSubPublisher(redistest.Client(t))
	deadline := time.Now().Add(20 * time.Second)
	for getAccount() != http.StatusUnauthorized {
		if time.Now().After(deadline) {
			t.Fatal("the revoked key still authenticates after the announcement, want 401")
		}
		if err := pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`)); err != nil {
			t.Fatalf("publish invalidation: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
