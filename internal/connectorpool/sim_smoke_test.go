package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/testutil/smscsim"
)

// TestSimSmokeEnroute is the step-130a harness smoke test: a message injected on mt.routed and addressed
// to a pool bound to a HEALTHY real simulator is submitted and recorded enroute, and the simulator's
// read-only observability confirms the submit_sm actually reached it. It proves the whole resilience
// harness end-to-end — the simulator launcher, the typed observability, startSimPool with its ordered
// teardown, and the CDR round-trip — before the fault scenarios (130b/130c) build on it.
func TestSimSmokeEnroute(t *testing.T) {
	const systemID, password, smscName = "smppclient1", "secret", "carrier"
	sim := smscsim.Launch(t, smscsim.HealthyConfig(systemID, password))

	// The pool binds once; wait for the bind to register on the simulator before injecting, so a failure
	// points at the injection/submit path rather than a race with the bind coming up.
	pool := startSimPool(t, simPoolConfig{BindAddr: sim.SMPPAddr, SystemID: systemID, Password: password})
	sim.WaitBindCount(t, smscName, 1, 15*time.Second)

	before, err := sim.Snapshot(context.Background(), smscName)
	if err != nil {
		t.Fatalf("snapshot before: %v", err)
	}

	id := pool.injectRouted(t, nil) // no fallback chain — a plain successful submit
	pool.waitCDRStatus(t, id, clickhouse.StatusEnroute, 20*time.Second)

	// The submit reached the simulator: its recorded-PDU counter advanced past the pre-injection baseline.
	sim.WaitRecordedPDUs(t, smscName, before.RecordedPDUs+1, 10*time.Second)
}
