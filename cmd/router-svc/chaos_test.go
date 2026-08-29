package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestRouterReadinessFollowsTheFailurePolicy freezes plan §1.5, which is a POLICY and not an accident:
// "router-svc avec Redis coupé reste ready (fail-closed sur le débit, messages durables dans Kafka)
// mais devient not ready si Kafka est injoignable".
//
// Both halves are asserted against the real graph, because each guards a different mistake:
//
//   - Redis cut, still ready. This is true today by ABSENCE — newOpsServer registers only a Kafka
//     check — which is exactly why it needs a test. Nothing else in the repo would notice someone
//     adding a Redis probe here, and that innocuous-looking line would pull every router pod out of
//     the load balancer during a Redis outage the router is designed to survive. Turning a degradation
//     into an outage is the failure this half exists to prevent.
//   - Kafka unreachable, not ready. Without it, "stays ready" could be satisfied by a service that
//     reports ready unconditionally — a probe that never fails is not a probe.
func TestRouterReadinessFollowsTheFailurePolicy(t *testing.T) {
	t.Run("stays ready when redis is cut", func(t *testing.T) {
		cfg := testConfig()
		cfg.OpsPort = 0 // Run picks the port; Addr() reports the bound one
		cfg.Postgres = pgtest.Config(t)
		cfg.Kafka.Brokers = kafkatest.Brokers(t)
		redisCfg, proxy := redistest.CuttableConfig(t)
		cfg.Redis = redisCfg

		// The graph opens its own Redis client from cfg and pings it on boot, so it must be built while
		// the link is still up — the cut comes after.
		app := buildRouter(t, cfg)

		if status, body := readyz(t, app); status != http.StatusOK {
			t.Fatalf("with redis up /readyz = %d %v, want 200 — the control failed", status, body)
		}

		proxy.Cut()

		if status, body := readyz(t, app); status != http.StatusOK {
			t.Errorf("with redis cut /readyz = %d %v, want 200: Redis is NOT a vital dependency of "+
				"router-svc (plan §1.5). Gating readiness on it drains every router pod during an "+
				"outage the router degrades through — the rate limiter falls back to its per-pod "+
				"ceiling and messages stay durable in Kafka", status, body)
		}
	})

	t.Run("goes not ready when kafka is unreachable", func(t *testing.T) {
		cfg := testConfig() // Kafka already points at a closed port
		cfg.OpsPort = 0
		cfg.Postgres = pgtest.Config(t)
		cfg.Redis = redistest.Config(t)

		app := buildRouter(t, cfg)

		status, body := readyz(t, app)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("with kafka unreachable /readyz = %d %v, want 503: Kafka IS vital — a router that "+
				"cannot consume mt.inbound must leave the load balancer", status, body)
		}
		if _, named := body["kafka"]; !named {
			t.Errorf("the readyz body must name the dependency that failed, got %v", body)
		}
	})
}

// buildRouter assembles the real router graph and serves its ops port until the test ends.
func buildRouter(t *testing.T, cfg config.Config) *routerApp {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	app, err := newRouterApp(ctx, cfg, silentLogger())
	if err != nil {
		cancel()
		t.Fatalf("newRouterApp: %v", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = app.ops.Run(ctx, time.Second) }()
	t.Cleanup(func() {
		cancel()
		<-done
		app.close()
	})
	return app
}

// readyz GETs /readyz on the app's ops port, returning the status and the per-dependency body. It
// retries only while the listener is still coming up — never on a served response, which is the answer.
func readyz(t *testing.T, app *routerApp) (int, map[string]string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		status, checks, err := getReadyz(t, app.ops.Addr())
		if err == nil {
			return status, checks
		}
		if time.Now().After(deadline) {
			t.Fatalf("ops port never answered /readyz: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func getReadyz(t *testing.T, addr string) (int, map[string]string, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var decoded struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, decoded.Checks, nil
}
