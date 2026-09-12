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

// TestSMPPServerReadinessGatesOnPostgres is the readiness half of step-260c's SMPP-bind row (guide de
// codage §16). internal/smppserver proves what a bind answers during the outage — ESME_RSYSERR, never
// ESME_RINVPASWD; this proves what the POD does, which plan §1.5 makes part of the same policy: "la
// readiness reflète les politiques de panne".
//
// The consequence is worth writing down because it is not the usual one. Every replica points at the
// same PostgreSQL, so they do not fail over — they all fail this probe at the same moment and leave
// the load balancer together. A dependency that merely degrades each bind into an ESME_RSYSERR
// becomes, through readiness, an ingress with no endpoints at all. That is a deliberate choice (a pod
// that cannot authenticate anyone has nothing to offer), and exactly the kind of choice that should be
// written down rather than rediscovered during an incident.
//
// PostgreSQL is this service's ONLY readiness probe (wiring.go:302-304 — Kafka and ClickHouse are
// deliberately not vital here), so the aggregate status is the assertion that carries the policy: 200
// with the link up, 503 without. The named check is asserted too, so the test cannot go green the day
// a second probe is added and starts answering for this one.
func TestSMPPServerReadinessGatesOnPostgres(t *testing.T) {
	cfg := testConfig()
	cfg.OpsPort = 0 // Run picks the port; Addr() reports the bound one
	cfg.Redis = redistest.Config(t)

	// The graph opens its own pool from the config and pings it on boot, so it is built while the link
	// is still up — the cut comes after.
	pgCfg, proxy := pgtest.CuttableConfig(t)
	cfg.Postgres = pgCfg

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	app, err := newSMPPApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newSMPPApp: %v", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = app.ops.Run(ctx, time.Second) }()
	t.Cleanup(func() {
		cancel()
		<-done
		app.close()
	})

	// Control: the pod is in the load balancer, and postgres is the check that says so.
	if status, got := smppReadyz(t, app); status != http.StatusOK || got != "ok" {
		t.Fatalf("with postgres up /readyz = %d, postgres = %q, want 200 and \"ok\" — the control failed",
			status, got)
	}

	proxy.Cut()

	status, got := smppReadyz(t, app)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("with postgres cut /readyz = %d (postgres = %q), want 503: PostgreSQL IS vital to "+
			"smpp-server-svc — every bind reads the credential table and there is no credential cache — "+
			"so a pod that cannot reach it refuses every bind and must leave the load balancer instead "+
			"of collecting ESMEs it can only turn away", status, got)
	}
	if got == "ok" {
		t.Errorf("/readyz is 503 but reports postgres = \"ok\": the 503 is coming from some other check, " +
			"so this test would keep passing the day postgres stops gating readiness at all")
	}

	proxy.Resume()

	// No latch: readiness must come back on its own, or a transient outage would need a rolling restart
	// to clear.
	deadline := time.Now().Add(20 * time.Second)
	for {
		status, got := smppReadyz(t, app)
		if status == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after postgres came back /readyz = %d (postgres = %q), want 200: readiness "+
				"latched on the outage and the pod would never rejoin the load balancer", status, got)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// smppReadyz GETs /readyz on the ops port and returns the aggregate status plus what the body says
// about postgres, failing the test if the dependency is not named at all — an unprobed dependency
// cannot gate anything.
//
// It retries only while the listener is still coming up, never on a served response, which is the
// answer. Addr() is re-read on every attempt: Run binds the ephemeral port, so until it does the
// address is still the unbound ":0" the config asked for.
func smppReadyz(t *testing.T, app *smppApp) (int, string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		status, checks, err := getReadyzChecks(t, app.ops.Addr())
		if err == nil {
			got, named := checks["postgres"]
			if !named {
				t.Fatalf("/readyz does not probe postgres at all (checks: %v): readiness cannot gate on "+
					"a dependency it never asks about, and this service cannot authenticate a single "+
					"bind without it", checks)
			}
			return status, got
		}
		if time.Now().After(deadline) {
			t.Fatalf("ops port never answered /readyz: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func getReadyzChecks(t *testing.T, addr string) (int, map[string]string, error) {
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
		Checks map[string]string `json:"checks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, decoded.Checks, nil
}
