package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// step-283: a submission beyond the account's throughput is refused before the acknowledgement. Until
// then the REST API acknowledged it and the router rejected it afterwards.
func TestASubmissionBeyondTheAccountRateIsRefusedBeforeTheAck(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.Kafka.Brokers = kafkatest.Brokers(t)
	pool := pgtest.Pool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "283-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := postgres.NewAccountRepo(pool).Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "283"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	key, hash, err := credential.GenerateAPIKey()
	if err != nil {
		t.Fatalf("generate api key: %v", err)
	}
	if _, err := postgres.NewCredentialRepo(pool).Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &hash,
	}); err != nil {
		t.Fatalf("create credential: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO control_plane.rate_limits (entity_type, entity_id, max_per_sec, burst_capacity)
		VALUES ('smpp_account', $1, 1, 1)`, account.ID); err != nil {
		t.Fatalf("insert rate limit: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM control_plane.rate_limits WHERE entity_id = $1`, account.ID)
	})

	app, err := newRestAPIApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRestAPIApp: %v", err)
	}
	defer app.close()
	api := httptest.NewServer(app.http.Handler)
	defer api.Close()

	submit := func() int {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{"to": "+2250700000001", "from": "INFO", "text": "hello"})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.URL+"/v1/messages", bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := api.Client().Do(req)
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if got := submit(); got != http.StatusAccepted {
		t.Fatalf("the first submission within the rate = %d, want 202 — the control failed", got)
	}
	// The bucket refills at 1/s and the first produce can take that long on a cold broker: a few tries
	// keep a slow first request from passing for an admission that let everything through.
	for range 5 {
		switch got := submit(); got {
		case http.StatusTooManyRequests:
			return
		case http.StatusAccepted:
		default:
			t.Fatalf("submission = %d, want 202 or 429", got)
		}
	}
	t.Fatal("five submissions past the account's 1/s were all acknowledged, want 429 before the acknowledgement")
}
