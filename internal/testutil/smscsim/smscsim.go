// Package smscsim launches the real SMSC simulator (github.com/martialanouman/go-smsc-simulator, a
// separate external project shipped as the smsc-simulator Docker image) for M8 resilience integration
// tests, replacing the in-repo fakesmsc peer for fault-injection scenarios.
//
// The simulator is config-file only: a YAML config (one or more virtual SMSCs, each with a scenario
// profile) is its sole input, and it exposes a READ-ONLY observability API — there is no runtime
// scenario mutation, so a test that needs a different behaviour launches a fresh simulator with a
// different config (startup < 2s). This harness provides that launcher plus config builders for the
// built-in scenario profiles; step-130 flips the resilience tests from fakesmsc to it.
package smscsim

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Image is the simulator image tag. It is a local/CI-built image (the simulator is a separate repo);
// `make smsc-sim` builds or loads it. A test skips cleanly when the image or Docker is unavailable.
const Image = "smsc-simulator:dev"

const (
	smppPort    = "2775/tcp" // plaintext SMPP listener of the (single) virtual SMSC
	controlPort = "9000/tcp" // read-only observability / control HTTP surface
	configPath  = "/etc/smsc/config.yml"
)

// Sim is a running simulator: its virtual-SMSC SMPP address (for binds) and its control-API base URL.
type Sim struct {
	SMPPAddr    string // host:port — dial this to bind/submit as connector-pool would
	ControlURL  string // http://host:port/v1 — read-only observability endpoints
	controlBase string // http://host:port — /health lives at the root, not under /v1
	container   testcontainers.Container
}

// Launch starts the simulator with the given YAML config and returns it, terminating the container via
// t.Cleanup. It skips the test when Docker or the simulator image is unavailable (so `go test` stays
// green on a laptop or in a CI without the image), rather than failing.
func Launch(t *testing.T, configYAML string) *Sim {
	t.Helper()
	if testing.Short() {
		t.Skip("smscsim: skipped under -short (needs Docker + the simulator image)")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	// Check image presence explicitly (the :dev tag lives in no registry) and skip if absent, rather
	// than inferring it from a launch error's text — an offline machine's pull failure must not turn a
	// missing image into a hard failure, and a genuine launch fault must not be mis-skipped.
	if !imagePresent(ctx) {
		t.Skipf("smscsim: image %q not available — run `make smsc-sim`", Image)
	}
	req := testcontainers.ContainerRequest{
		Image:        Image,
		ExposedPorts: []string{smppPort, controlPort},
		Files: []testcontainers.ContainerFile{{
			Reader:            strings.NewReader(configYAML),
			ContainerFilePath: configPath,
			FileMode:          0o644,
		}},
		WaitingFor: wait.ForHTTP("/health").WithPort(controlPort).WithStartupTimeout(30 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("smscsim: start: %v", err) // image presence was checked above → any error here is real
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("smscsim: host: %v", err)
	}
	smpp, err := c.MappedPort(ctx, smppPort)
	if err != nil {
		t.Fatalf("smscsim: smpp port: %v", err)
	}
	ctrl, err := c.MappedPort(ctx, controlPort)
	if err != nil {
		t.Fatalf("smscsim: control port: %v", err)
	}
	base := fmt.Sprintf("http://%s:%s", host, ctrl.Port())
	return &Sim{
		SMPPAddr:    fmt.Sprintf("%s:%s", host, smpp.Port()),
		ControlURL:  base + "/v1",
		controlBase: base,
		container:   c,
	}
}

// imagePresent reports whether the simulator image exists locally, via `docker image inspect` (exit 0
// iff present; respects DOCKER_HOST). A definitive check so Launch skips on a genuinely absent image
// rather than inferring absence from a launch error's text (which would turn an offline pull failure
// into a hard failure). A missing docker CLI yields a non-nil error → false → a conservative skip.
func imagePresent(ctx context.Context) bool {
	return exec.CommandContext(ctx, "docker", "image", "inspect", Image).Run() == nil
}

// HealthyConfig is a one-virtual-SMSC config on the nominal profile (100% success, low latency), for a
// smoke test or a happy-path resilience baseline.
func HealthyConfig(systemID, password string) string {
	return virtualSMSC(systemID, password, `    scenario:
      profile: healthy
      latency:
        distribution: fixed
        params: { ms: 5 }
`, "")
}

// ThrottlingConfig is a one-virtual-SMSC config that returns ESME_RTHROTTLED beyond capPerSec, to
// exercise the gateway's adaptive throttling.
func ThrottlingConfig(systemID, password string, capPerSec int) string {
	scenario := fmt.Sprintf(`    scenario:
      profile: throttling-carrier
      params:
        throughput_cap_per_sec: %d
        error_code: ESME_RTHROTTLED
      latency:
        distribution: fixed
        params: { ms: 5 }
`, capPerSec)
	return virtualSMSC(systemID, password, scenario, fmt.Sprintf("    throughput_limit_per_sec: %d\n", capPerSec))
}

// DeadCarrierConfig is a one-virtual-SMSC config on the dead-carrier profile: it fails every submit_sm
// (a connector-health fault), to drive the circuit breaker open and exercise fallback/reroute.
func DeadCarrierConfig(systemID, password string) string {
	return virtualSMSC(systemID, password, `    scenario:
      profile: dead-carrier
      latency:
        distribution: fixed
        params: { ms: 5 }
`, "")
}

// FlakyCarrierConfig is a one-virtual-SMSC config on the flaky-carrier profile: intermittent failures,
// for a breaker that opens and closes rather than staying pinned.
func FlakyCarrierConfig(systemID, password string) string {
	return virtualSMSC(systemID, password, `    scenario:
      profile: flaky-carrier
      latency:
        distribution: fixed
        params: { ms: 5 }
`, "")
}

// SlowCarrierConfig is a one-virtual-SMSC config on the slow-carrier profile: every submit is delayed by
// latencyMs, to exercise a bind pool's throughput (a larger pool hides the per-submit latency). The
// simulator constrains slow-carrier latency to [2000,4000] ms.
func SlowCarrierConfig(systemID, password string, latencyMs int) string {
	scenario := fmt.Sprintf(`    scenario:
      profile: slow-carrier
      latency:
        distribution: fixed
        params: { ms: %d }
`, latencyMs)
	return virtualSMSC(systemID, password, scenario, "")
}

// virtualSMSC assembles a single-virtual-SMSC config from the shared preamble, an extra field block,
// and the scenario block.
func virtualSMSC(systemID, password, scenarioBlock, extra string) string {
	return fmt.Sprintf(`observability:
  http_port: 9000

virtual_smscs:
  - name: carrier
    port: 2775
    bind_credentials:
      system_id: %q
      password: %q
    addr_ton: 1
    addr_npi: 1
    address_range: ".*"
    tls:
      enabled: false
    seed: 42
    pdu_buffer_size: 10000
%s%s`, systemID, password, extra, scenarioBlock)
}

// Health reports whether the simulator's read-only control API answers GET /health (root) with 200.
func (s *Sim) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.controlBase+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("smscsim: /health status %d", resp.StatusCode)
	}
	return nil
}

// VirtualSMSCs returns the raw JSON of GET /v1/virtual-smscs (the read-only inventory), for a test to
// assert the simulator is hosting the configured SMSC.
func (s *Sim) VirtualSMSCs(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.ControlURL+"/virtual-smscs", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("smscsim: /virtual-smscs status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// VirtualSMSC is one virtual SMSC's read-only observability snapshot from GET /v1/virtual-smscs. The
// counters are what a resilience test asserts against: BindCount rises as the connector pool establishes
// its binds and falls when they drop; RecordedPDUs is the total submit_sm (and other PDUs) the SMSC has
// seen, so a test can prove traffic actually reached a connector.
type VirtualSMSC struct {
	Name          string `json:"name"`
	Port          int    `json:"port"`
	ActiveProfile string `json:"active_profile"`
	BindCount     int    `json:"bind_count"`
	LogicalClock  int64  `json:"logical_clock"`
	RecordedPDUs  int    `json:"recorded_pdus"`
}

// Inventory parses GET /v1/virtual-smscs into the typed snapshots.
func (s *Sim) Inventory(ctx context.Context) ([]VirtualSMSC, error) {
	raw, err := s.VirtualSMSCs(ctx)
	if err != nil {
		return nil, err
	}
	var out []VirtualSMSC
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("smscsim: decode inventory: %w", err)
	}
	return out, nil
}

// Snapshot returns the named virtual SMSC's current counters, erroring if it is absent.
func (s *Sim) Snapshot(ctx context.Context, name string) (VirtualSMSC, error) {
	inv, err := s.Inventory(ctx)
	if err != nil {
		return VirtualSMSC{}, err
	}
	for _, v := range inv {
		if v.Name == name {
			return v, nil
		}
	}
	return VirtualSMSC{}, fmt.Errorf("smscsim: virtual smsc %q not found", name)
}

// WaitBindCount polls until the named SMSC reports at least want binds, or fails the test at the
// deadline. Used to prove a pool bound (want=poolSize) or reconnected (bind_count recovered) rather than
// sleeping for a fixed guess.
func (s *Sim) WaitBindCount(t *testing.T, name string, want int, within time.Duration) {
	t.Helper()
	s.waitCounter(t, name, within, func(v VirtualSMSC) bool { return v.BindCount >= want },
		func(v VirtualSMSC) string { return fmt.Sprintf("bind_count=%d, want >= %d", v.BindCount, want) })
}

// WaitRecordedPDUs polls until the named SMSC has recorded at least want PDUs, or fails at the deadline —
// proving traffic reached the connector (e.g. all rerouted messages landed on the fallback SMSC).
func (s *Sim) WaitRecordedPDUs(t *testing.T, name string, want int, within time.Duration) {
	t.Helper()
	s.waitCounter(t, name, within, func(v VirtualSMSC) bool { return v.RecordedPDUs >= want },
		func(v VirtualSMSC) string { return fmt.Sprintf("recorded_pdus=%d, want >= %d", v.RecordedPDUs, want) })
}

// waitCounter polls the named SMSC's snapshot until cond holds or within elapses, failing with the last
// observed state (rendered by describe). A read error is transient (the sim may be mid-request), retried.
func (s *Sim) waitCounter(t *testing.T, name string, within time.Duration, cond func(VirtualSMSC) bool, describe func(VirtualSMSC) string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(within)
	var last VirtualSMSC
	read := false
	var lastErr error
	for time.Now().Before(deadline) {
		v, err := s.Snapshot(ctx, name)
		if err == nil {
			last, read = v, true
			if cond(v) {
				return
			}
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !read {
		t.Fatalf("smscsim: %q never read a snapshot within %s (last error: %v)", name, within, lastErr)
	}
	t.Fatalf("smscsim: %q never satisfied condition within %s (last: %s)", name, within, describe(last))
}
