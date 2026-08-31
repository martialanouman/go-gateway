package observability_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/observability"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		ServiceName:     "router-svc",
		Environment:     config.EnvDevelopment,
		LogLevel:        "info",
		OpsPort:         0, // port 0: the OS picks a free one, so tests never collide
		ShutdownTimeout: 5 * time.Second,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// startOps runs an ops server on an ephemeral port and returns its base URL. It fails the test if
// the server does not come up, and stops it on cleanup.
func startOps(t *testing.T, checks ...observability.ReadinessCheck) string {
	t.Helper()

	ops, err := observability.NewOpsServer(testConfig(t), discardLogger(), checks...)
	if err != nil {
		t.Fatalf("NewOpsServer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ops.Run(ctx, 2*time.Second) }()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("ops server did not shut down within 5s")
		}
	})

	base := "http://" + waitForAddr(t, ops)
	waitReachable(t, base+"/healthz")
	return base
}

// waitForAddr polls until Run has bound its listener and published the real address.
func waitForAddr(t *testing.T, ops *observability.OpsServer) string {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if addr := ops.Addr(); !strings.HasSuffix(addr, ":0") {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("ops server never bound a port")
	return ""
}

func waitReachable(t *testing.T, url string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx // test-local probe against a loopback server
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never became reachable", url)
}

func get(t *testing.T, url string) (int, []byte) {
	t.Helper()

	resp, err := http.Get(url) //nolint:noctx // test-local probe against a loopback server
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body of %s: %v", url, err)
	}
	return resp.StatusCode, body
}

func okCheck(name string) observability.ReadinessCheck {
	return observability.ReadinessCheck{
		Name:  name,
		Probe: func(context.Context) error { return nil },
	}
}

func failCheck(name string, err error) observability.ReadinessCheck {
	return observability.ReadinessCheck{
		Name:  name,
		Probe: func(context.Context) error { return err },
	}
}

// TestHealthzIsAlwaysOK is the liveness contract (plan §1.5): /healthz checks NOTHING, so a
// dependency outage can never restart the pod. It answers 200 even with every check failing.
func TestHealthzIsAlwaysOK(t *testing.T) {
	base := startOps(t,
		failCheck("kafka", errors.New("connection refused")),
		failCheck("postgres", errors.New("connection refused")),
	)

	code, body := get(t, base+"/healthz")
	if code != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200 even with every dependency down: %s", code, body)
	}
	if !json.Valid(body) {
		t.Errorf("/healthz body is not JSON: %s", body)
	}
}

// TestReadyzReflectsVitalDependencies is the readiness contract, and the M0 acceptance criterion:
// 200 when the vital dependencies answer, 503 when Kafka is gone.
func TestReadyzReflectsVitalDependencies(t *testing.T) {
	t.Run("all checks pass", func(t *testing.T) {
		base := startOps(t, okCheck("kafka"), okCheck("postgres"))

		code, body := get(t, base+"/readyz")
		if code != http.StatusOK {
			t.Fatalf("GET /readyz = %d, want 200: %s", code, body)
		}

		var resp struct {
			Status string            `json:"status"`
			Checks map[string]string `json:"checks"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode body: %v (%s)", err, body)
		}
		if resp.Status != "ok" {
			t.Errorf("status = %q, want ok", resp.Status)
		}
		if resp.Checks["kafka"] != "ok" || resp.Checks["postgres"] != "ok" {
			t.Errorf("checks = %v, want both ok", resp.Checks)
		}
	})

	t.Run("kafka down means 503", func(t *testing.T) {
		base := startOps(t,
			failCheck("kafka", errors.New("dial localhost:9092: connection refused")),
			okCheck("postgres"),
		)

		code, body := get(t, base+"/readyz")
		if code != http.StatusServiceUnavailable {
			t.Fatalf("GET /readyz = %d, want 503 with Kafka down: %s", code, body)
		}

		var resp struct {
			Status string            `json:"status"`
			Checks map[string]string `json:"checks"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode body: %v (%s)", err, body)
		}
		if resp.Status != "unavailable" {
			t.Errorf("status = %q, want unavailable", resp.Status)
		}
		if !strings.Contains(resp.Checks["kafka"], "connection refused") {
			t.Errorf("checks[kafka] = %q, should say why it failed", resp.Checks["kafka"])
		}
		// The healthy dependency must still be reported, or an operator cannot tell which one
		// is at fault.
		if resp.Checks["postgres"] != "ok" {
			t.Errorf("checks[postgres] = %q, want ok", resp.Checks["postgres"])
		}
	})

	t.Run("no checks means ready", func(t *testing.T) {
		base := startOps(t)

		code, body := get(t, base+"/readyz")
		if code != http.StatusOK {
			t.Errorf("GET /readyz = %d, want 200 with no vital dependency: %s", code, body)
		}
	})
}

// TestReadyzRecoversWhenDependencyReturns: readiness is a live signal, not a boot-time verdict.
// A pod removed from the load balancer must come back on its own once the dependency does.
func TestReadyzRecoversWhenDependencyReturns(t *testing.T) {
	var down atomic.Bool
	down.Store(true)

	base := startOps(t, observability.ReadinessCheck{
		Name: "kafka",
		Probe: func(context.Context) error {
			if down.Load() {
				return errors.New("connection refused")
			}
			return nil
		},
	})

	if code, body := get(t, base+"/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz = %d, want 503 while down: %s", code, body)
	}

	down.Store(false)

	if code, body := get(t, base+"/readyz"); code != http.StatusOK {
		t.Fatalf("GET /readyz = %d, want 200 once recovered: %s", code, body)
	}
}

// TestReadyzProbesRunConcurrently: probes are independent, so the endpoint should cost about one
// probe, not their sum. With four 150ms probes, a serial implementation needs 600ms.
func TestReadyzProbesRunConcurrently(t *testing.T) {
	const (
		probes    = 4
		probeTime = 150 * time.Millisecond
	)

	checks := make([]observability.ReadinessCheck, 0, probes)
	for i := range probes {
		checks = append(checks, observability.ReadinessCheck{
			Name: fmt.Sprintf("dep-%d", i),
			Probe: func(ctx context.Context) error {
				select {
				case <-time.After(probeTime):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		})
	}
	base := startOps(t, checks...)

	start := time.Now()
	code, body := get(t, base+"/readyz")
	elapsed := time.Since(start)

	if code != http.StatusOK {
		t.Fatalf("GET /readyz = %d, want 200: %s", code, body)
	}
	if serial := probes * probeTime; elapsed >= serial {
		t.Errorf("readiness took %s; %d probes of %s ran serially (>= %s)", elapsed, probes, probeTime, serial)
	}
}

// TestReadyzBoundsAHangingProbe: a probe that never returns must not hang the endpoint. A kubelet
// that gets no answer calls the pod unready anyway — answering quickly is strictly better.
func TestReadyzBoundsAHangingProbe(t *testing.T) {
	base := startOps(t, observability.ReadinessCheck{
		Name: "hangs",
		Probe: func(ctx context.Context) error {
			<-ctx.Done() // never returns on its own
			return ctx.Err()
		},
	})

	start := time.Now()
	code, _ := get(t, base+"/readyz")
	elapsed := time.Since(start)

	if code != http.StatusServiceUnavailable {
		t.Errorf("GET /readyz = %d, want 503 for a hanging probe", code)
	}
	if elapsed > 10*time.Second {
		t.Errorf("readiness took %s; a hanging probe must be bounded", elapsed)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	base := startOps(t)

	code, body := get(t, base+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", code)
	}
	// The Go and process collectors are pre-registered, so the exposition is never empty.
	if !strings.Contains(string(body), "go_goroutines") {
		t.Errorf("/metrics should expose the Go collector; got:\n%s", truncate(string(body), 400))
	}
}

// TestRegistryIsUsableByServices: services register their own collectors on the server's
// registry, so what they register must actually appear on /metrics.
func TestRegistryIsUsableByServices(t *testing.T) {
	ops, err := observability.NewOpsServer(testConfig(t), discardLogger())
	if err != nil {
		t.Fatalf("NewOpsServer() error = %v", err)
	}
	if ops.Registry() == nil {
		t.Fatal("Registry() = nil; services have nowhere to register their metrics")
	}
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "probe_total", Help: "h"},
		[]string{"connector_id"})
	if err := ops.Registry().Register(counter); err != nil {
		t.Fatalf("a bounded collector was refused: %v", err)
	}
}

// TestRegistryEnforcesTheCardinalityGuard is what keeps the guard wired in. NewOpsServer is the ONE line that
// makes it effective across all eight services, and nothing else would notice its removal: every caller only
// uses MustRegister, so swapping the guarded registry back for a plain prometheus.Registry compiles cleanly
// and leaves every other test green — with the guard silently off in production.
func TestRegistryEnforcesTheCardinalityGuard(t *testing.T) {
	ops, err := observability.NewOpsServer(testConfig(t), discardLogger())
	if err != nil {
		t.Fatalf("NewOpsServer() error = %v", err)
	}

	leaky := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "leaky_total",
		Help: "one series per destination number",
	}, []string{"msisdn"})

	if err := ops.Registry().Register(leaky); err == nil {
		t.Fatal("the ops registry accepted an msisdn label: the cardinality guard is not wired in")
	}
}

func TestNewOpsServerRejectsBadChecks(t *testing.T) {
	tests := []struct {
		name   string
		checks []observability.ReadinessCheck
	}{
		{"unnamed check", []observability.ReadinessCheck{{Probe: func(context.Context) error { return nil }}}},
		{"nil probe", []observability.ReadinessCheck{{Name: "kafka"}}},
		{"duplicate names", []observability.ReadinessCheck{okCheck("kafka"), okCheck("kafka")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := observability.NewOpsServer(testConfig(t), discardLogger(), tc.checks...); err == nil {
				t.Error("NewOpsServer() succeeded, want an error")
			}
		})
	}
}

// TestRunStopsOnContextCancel is the graceful-drain contract: SIGTERM cancels the context, and
// Run must return cleanly — a cancelled context is how a service stops, not an error.
func TestRunStopsOnContextCancel(t *testing.T) {
	ops, err := observability.NewOpsServer(testConfig(t), discardLogger())
	if err != nil {
		t.Fatalf("NewOpsServer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ops.Run(ctx, 2*time.Second) }()

	waitReachable(t, "http://"+waitForAddr(t, ops)+"/healthz")
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() = %v, want nil on a cancelled context", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return within 5s of cancellation")
	}
}

// TestRunReportsPortInUse: a service whose ops port is taken must fail loudly at startup rather
// than run without health endpoints. The port is occupied on the wildcard address, because that
// is what the ops server binds — occupying 127.0.0.1 only would not conflict with it.
func TestRunReportsPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	cfg := testConfig(t)
	cfg.OpsPort = ln.Addr().(*net.TCPAddr).Port

	ops, err := observability.NewOpsServer(cfg, discardLogger())
	if err != nil {
		t.Fatalf("NewOpsServer() error = %v", err)
	}

	// Bounded: if Run wrongly succeeds it would otherwise serve forever and hang the suite.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := ops.Run(ctx, time.Second); err == nil {
		t.Error("Run() succeeded on an occupied port, want an error")
	}
}

func TestTCPDialCheck(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()

	t.Run("reachable", func(t *testing.T) {
		check := observability.TCPDialCheck("dep", addr, time.Second)
		if check.Name != "dep" {
			t.Errorf("Name = %q, want dep", check.Name)
		}
		if err := check.Probe(context.Background()); err != nil {
			t.Errorf("Probe() = %v, want nil for a listening port", err)
		}
	})

	t.Run("unreachable", func(t *testing.T) {
		_ = ln.Close() // now nothing is listening

		check := observability.TCPDialCheck("dep", addr, 500*time.Millisecond)
		if err := check.Probe(context.Background()); err == nil {
			t.Error("Probe() = nil, want an error once the listener is gone")
		}
	})
}

// TestAnyTCPDialCheck covers the Kafka shape: one broker down is still a working cluster, and
// going unready over it would drain every pod during a routine broker restart.
func TestAnyTCPDialCheck(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	live := ln.Addr().String()

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	tests := []struct {
		name    string
		addrs   []string
		wantErr bool
	}{
		{"all reachable", []string{live}, false},
		{"one of several down", []string{deadAddr, live}, false},
		{"live one listed first", []string{live, deadAddr}, false},
		{"all down", []string{deadAddr}, true},
		{"none configured", nil, true},
		// The case the closed ports above cannot cover: a broker that swallows the SYN instead of
		// refusing it, which is what a firewall or a partition looks like. Dialling sequentially on
		// one shared budget lets it eat the whole timeout and reports the live broker — dialled
		// next on an already-expired context — as unreachable too.
		// 192.0.2.1 is RFC 5737 TEST-NET-1, unroutable by definition. On a network that answers it
		// with a prompt ICMP unreachable this case passes either way; it proves the fix where it
		// can and never false-fails.
		{"blackholed broker listed first", []string{"192.0.2.1:9092", live}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			check := observability.AnyTCPDialCheck("kafka", tc.addrs, 500*time.Millisecond)
			err := check.Probe(context.Background())
			if tc.wantErr && err == nil {
				t.Error("Probe() = nil, want an error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Probe() = %v, want nil", err)
			}
		})
	}
}

func TestAnyTCPDialCheckHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	check := observability.AnyTCPDialCheck("kafka", []string{"192.0.2.1:9092"}, 5*time.Second)
	if err := check.Probe(ctx); err == nil {
		t.Error("Probe() = nil on a cancelled context, want an error")
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// startOpsWithServer is startOps plus the server handle, for the tests that must act on the pod's
// lifecycle rather than on its dependencies.
func startOpsWithServer(t *testing.T, checks ...observability.ReadinessCheck) (string, *observability.OpsServer) {
	t.Helper()

	ops, err := observability.NewOpsServer(testConfig(t), discardLogger(), checks...)
	if err != nil {
		t.Fatalf("NewOpsServer() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ops.Run(ctx, 2*time.Second) }()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("ops server did not shut down within 5s")
		}
	})

	base := "http://" + waitForAddr(t, ops)
	waitReachable(t, base+"/healthz")
	return base, ops
}

// TestReadyzGoesNotReadyOnDrain: once the drain starts, the pod reports NOT ready even though every
// vital dependency is healthy.
//
// Both halves matter. The healthy-checks control comes first, because a 503 from a broken dependency
// would satisfy the drain assertion for entirely the wrong reason. And the checks stay healthy on
// purpose: readiness during a drain is a statement about THIS POD's lifecycle, not about Postgres.
// A draining pod that keeps answering 200 stays in the Service endpoints and keeps being handed new
// binds while it closes the listener underneath them — the race this exists to close.
func TestReadyzGoesNotReadyOnDrain(t *testing.T) {
	base, ops := startOpsWithServer(t, okCheck("kafka"), okCheck("postgres"))

	if code, body := get(t, base+"/readyz"); code != http.StatusOK {
		t.Fatalf("before the drain GET /readyz = %d, want 200: %s — the control failed", code, body)
	}

	ops.BeginDrain()

	code, body := get(t, base+"/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("while draining GET /readyz = %d, want 503 (dependencies are healthy — a pod that "+
			"is going away is not ready, whatever Postgres says): %s", code, body)
	}

	var resp struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode body: %v (%s)", err, body)
	}
	if resp.Status != "draining" {
		t.Errorf("status = %q, want %q: an operator reading a 503 must be able to tell a shutdown "+
			"apart from a dependency outage", resp.Status, "draining")
	}
}

// TestHealthzStaysOKWhileDraining: liveness must NOT follow readiness here. /healthz failing during a
// drain would make the kubelet restart the very pod that is deliberately going away (plan §1.5:
// liveness failure → restart, readiness failure → LB removal).
func TestHealthzStaysOKWhileDraining(t *testing.T) {
	base, ops := startOpsWithServer(t, okCheck("kafka"))

	ops.BeginDrain()

	if code, body := get(t, base+"/healthz"); code != http.StatusOK {
		t.Errorf("while draining GET /healthz = %d, want 200: a draining pod is alive, and failing "+
			"liveness would have the kubelet restart it mid-drain: %s", code, body)
	}
}

// TestDrainHookMarksNotReadyThenWaits: the hook flips readiness FIRST and only then spends the delay.
// Waiting before flipping would burn the grace period while still advertising the pod as ready, which
// is the same bug as not waiting at all.
func TestDrainHookMarksNotReadyThenWaits(t *testing.T) {
	base, ops := startOpsWithServer(t, okCheck("kafka"))

	const delay = 150 * time.Millisecond
	flipped := make(chan struct{})
	go func() {
		// Observe readiness while the hook is still inside its wait.
		defer close(flipped)
		deadline := time.Now().Add(delay)
		for time.Now().Before(deadline) {
			if code, _ := get(t, base+"/readyz"); code == http.StatusServiceUnavailable {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Error("readiness never went 503 while the drain hook was still waiting: the hook waited " +
			"before marking the pod not-ready, so the grace period was spent advertising it as ready")
	}()

	start := time.Now()
	ops.DrainHook(delay)(context.Background())
	elapsed := time.Since(start)

	<-flipped
	if elapsed < delay {
		t.Errorf("drain hook returned after %v, want at least %v: without the wait the listener "+
			"closes before kube-proxy has removed the endpoint, and the flip buys nothing",
			elapsed, delay)
	}
}
