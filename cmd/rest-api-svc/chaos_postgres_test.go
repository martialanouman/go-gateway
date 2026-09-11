package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestRestAPIReadinessGatesOnPostgres is the readiness half of step-260c's REST-auth row (guide de
// codage §16). internal/restapi proves what a request answers during the outage — 500, never 401;
// this proves what the POD does, which plan §1.5 makes part of the same policy: "la readiness reflète
// les politiques de panne".
//
// The consequence is worth writing down because it is not the usual one. Every replica points at the
// same PostgreSQL, so they do not fail over — they all fail this probe at the same moment and leave
// the load balancer together. A dependency that merely degrades each request into a 500 becomes,
// through readiness, a service with no endpoints at all. That is a deliberate choice (a pod that
// cannot authenticate anyone has nothing to offer), and exactly the kind of choice that should be
// written down rather than rediscovered during an incident.
//
// The assertion is on the NAMED check rather than the aggregate status, and deliberately so: this
// service probes three dependencies (wiring.go:187-191) and two of them — Kafka and ClickHouse — sit
// at closed ports in testConfig, so /readyz is already 503 before anything is cut. Bringing them up
// would cost this package a Redpanda and a ClickHouse container for a fact the per-dependency body
// states directly: that postgres is one of the checks readiness gates on, and that it reports the
// outage rather than staying "ok" off some cached verdict. (Its twin in cmd/smpp-server-svc can assert
// the aggregate, Postgres being that service's only probe.)
func TestRestAPIReadinessGatesOnPostgres(t *testing.T) {
	cfg := testConfig()
	cfg.OpsPort = 0 // Run picks the port; Addr() reports the bound one
	cfg.Redis = redistest.Config(t)

	// The graph opens its own pool from the config and pings it on boot, so it is built while the link
	// is still up — the cut comes after.
	pgCfg, proxy := pgtest.CuttableConfig(t)
	cfg.Postgres = pgCfg

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	app, err := newRestAPIApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newRestAPIApp: %v", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = app.ops.Run(ctx, time.Second) }()
	t.Cleanup(func() {
		cancel()
		<-done
		app.close()
	})

	// Control: postgres is among the checks, and it passes. Without it, "postgres is not ok" under the
	// cut would hold just as well for a dependency nobody ever probes.
	if got := restReadyzCheck(t, app); got != "ok" {
		t.Fatalf("with postgres up /readyz reports postgres = %q, want \"ok\" — the control failed", got)
	}

	proxy.Cut()

	if got := restReadyzCheck(t, app); got == "ok" {
		t.Fatal("with postgres cut /readyz still reports postgres = \"ok\": PostgreSQL IS vital to " +
			"rest-api-svc — every authenticated request reads the api_keys table and there is no " +
			"principal cache — so a pod that cannot reach it serves nothing but 500s and must leave the " +
			"load balancer instead of taking traffic it cannot answer")
	}

	proxy.Resume()

	// No latch: readiness must come back on its own, or a transient outage would need a rolling restart
	// to clear.
	deadline := time.Now().Add(20 * time.Second)
	for {
		got := restReadyzCheck(t, app)
		if got == "ok" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after postgres came back /readyz still reports postgres = %q: readiness latched "+
				"on the outage and the pod would never rejoin the load balancer", got)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// restReadyzCheck GETs /readyz on the ops port and returns what the body says about postgres, failing
// the test if the dependency is not named at all — an unprobed dependency cannot gate anything.
//
// It retries only while the listener is still coming up, never on a served response, which is the
// answer. Addr() is re-read on every attempt: Run binds the ephemeral port, so until it does the
// address is still the unbound ":0" the config asked for.
func restReadyzCheck(t *testing.T, app *restAPIApp) string {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		checks, err := getReadyzChecks(t, app.ops.Addr())
		if err == nil {
			got, named := checks["postgres"]
			if !named {
				t.Fatalf("/readyz does not probe postgres at all (checks: %v): readiness cannot gate on "+
					"a dependency it never asks about, and this service cannot authenticate a single "+
					"caller without it", checks)
			}
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("ops port never answered /readyz: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func getReadyzChecks(t *testing.T, addr string) (map[string]string, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var decoded struct {
		Checks map[string]string `json:"checks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, err
	}
	return decoded.Checks, nil
}
