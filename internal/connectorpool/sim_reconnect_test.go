package connectorpool_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/testutil/smscsim"
	"github.com/martialanouman/go-gateway/internal/testutil/tcpproxy"
)

// bindPool is a pool wired just for the bind-lifecycle scenarios: no Kafka/ClickHouse, a blocking consumer
// so the pool stays alive on a healthy bind, and Run's return captured so a test can assert it stops (a
// permanent reject) or keeps running (a recoverable drop).
type bindPool struct {
	svc   *connectorpool.Service
	errCh chan error
}

// startBindPool builds and runs a pool against bindAddr with the given credentials and reconnect policy,
// returning it. Teardown cancels Run via t.Cleanup.
func startBindPool(t *testing.T, bindAddr, systemID, password string, rc reconnect.Config) *bindPool {
	t.Helper()
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:    blockingConsumer{},
		CDR:         &fakeCDR{},
		ConnectorID: uuid.New(),
		Bind: connectorpool.BindConfig{
			Addr: bindAddr, SystemID: systemID, Password: password,
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Reconnect: rc,
		Tracer:    observability.Tracer(nil, "connector-sim"),
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1) // buffered: the send never blocks even if the test never reads it
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); errCh <- svc.Run(ctx) }()
	// Ordered teardown: cancel Run and JOIN its goroutine (via the WaitGroup, which works whether or not
	// the test already consumed errCh) before the simulator container is terminated.
	t.Cleanup(func() { cancel(); wg.Wait() })
	return &bindPool{svc: svc, errCh: errCh}
}

// waitLink polls the pool's LinkStatus until it equals want, or fails at the deadline.
func (p *bindPool) waitLink(t *testing.T, want string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last string
	for time.Now().Before(deadline) {
		last = p.svc.LinkStatus()
		if last == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("LinkStatus never became %q within %s (last: %q)", want, within, last)
}

// TestSimBindRejectStopsAutoRetry is the step-130c acceptance scenario (plan §12): a wrong password —
// ESME_RINVPASWD — must STOP the auto-reconnection loop, not churn against the SMSC forever. Auto-reconnect
// is deliberately ENABLED: a handshake reject is a permanent fault (not a live-link drop), so the loop
// gives up and Run returns. This exercises the real simulator's reject-then-close (a bodyless error
// bind_resp + immediate socket close) end-to-end — the exact behaviour the codec/roundtrip fix addressed.
func TestSimBindRejectStopsAutoRetry(t *testing.T) {
	sim := smscsim.Launch(t, smscsim.HealthyConfig("right-id", "right-pw"))
	pool := startBindPool(t, sim.SMPPAddr, "right-id", "WRONG-pw", reconnect.Config{
		Enabled: true, InitialDelay: 50 * time.Millisecond, Multiplier: 2, MaxDelay: 500 * time.Millisecond,
	})

	select {
	case err := <-pool.errCh:
		var rej *connectorpool.BindRejectedError
		if !errors.As(err, &rej) {
			t.Fatalf("Run error = %v, want a BindRejectedError (a reject must be permanent, not retried)", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run did not stop on a wrong-password reject within 15s — it is churning the SMSC")
	}

	if snap, err := sim.Snapshot(context.Background(), "carrier"); err == nil && snap.BindCount != 0 {
		t.Errorf("sim bind_count = %d, want 0 (the bind was rejected)", snap.BindCount)
	}
	if pool.svc.LinkStatus() == "up" {
		t.Errorf("LinkStatus = up, want down (the bind was rejected)")
	}
}

// TestSimReconnectRecovers is the step-130c acceptance scenario (plan §12): a bind that DROPS (a link
// fault, not a reject) with auto-reconnect ON comes back on its own. The pool binds the simulator through
// an in-process TCP proxy; cutting the proxy severs the live link (the pool goes down and retries), and
// resuming it lets the pool re-dial and re-bind — LinkStatus goes up → down → up and the simulator sees a
// fresh bind. The proxy (not a container restart) keeps the bind address stable across the cut.
func TestSimReconnectRecovers(t *testing.T) {
	sim := smscsim.Launch(t, smscsim.HealthyConfig("live", "pw"))
	proxy := tcpproxy.New(t, sim.SMPPAddr)
	pool := startBindPool(t, proxy.Addr(), "live", "pw", reconnect.Config{
		Enabled: true, InitialDelay: 100 * time.Millisecond, Multiplier: 1.5, MaxDelay: time.Second,
	})

	pool.waitLink(t, "up", 15*time.Second)
	sim.WaitBindCount(t, "carrier", 1, 15*time.Second)

	// Sever the link: the pool's bind drops and it starts retrying (through the still-cut proxy). While a
	// bind is down but auto-reconnect is retrying, the link reports "reconnecting" (distinct from a final
	// "down"); it stays there as long as the proxy refuses the re-dials.
	proxy.Cut()
	pool.waitLink(t, "reconnecting", 10*time.Second)

	// Restore the link: the reconnect loop re-dials and re-binds on its own.
	proxy.Resume()
	pool.waitLink(t, "up", 15*time.Second)
	sim.WaitBindCount(t, "carrier", 1, 15*time.Second) // the sim sees the fresh bind
}

// TestSimNoReconnectStaysDown is the other half of §12's reconnect scenario: WITHOUT auto-reconnect, a
// dropped bind is not retried — Run returns (surfacing the drop so the supervisor / a manual rebind
// decides), and the link stays down.
func TestSimNoReconnectStaysDown(t *testing.T) {
	sim := smscsim.Launch(t, smscsim.HealthyConfig("live", "pw"))
	proxy := tcpproxy.New(t, sim.SMPPAddr)
	// Reconnect zero value = disabled.
	pool := startBindPool(t, proxy.Addr(), "live", "pw", reconnect.Config{})

	pool.waitLink(t, "up", 15*time.Second)

	proxy.Cut()
	select {
	case err := <-pool.errCh:
		if err == nil {
			t.Fatal("Run returned nil on a dropped bind, want the drop surfaced (reconnect disabled)")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return on a dropped bind with reconnect disabled — it should not retry")
	}
	if pool.svc.LinkStatus() == "up" {
		t.Errorf("LinkStatus = up after the link was cut, want down")
	}
}
