package connectorpool_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// TestConnectorUnbindsOnDrain guards the one third of the guide de codage §5 [MUST] that this repo
// already honoured: "connector-pool-svc termine les submit_sm en vol dans la fenêtre" — its binds take
// their leave instead of dropping the TCP connection.
//
// It was the untested third, which is why it is here. connectorpool.Run detaches the unbind on purpose
// (the write must happen AFTER ctx is cancelled, since cancellation is what starts the drain), and that
// detach is exactly the kind of deliberate subtlety a later refactor removes as dead weight — a
// //nolint:contextcheck sitting next to a context that is already cancelled reads like a mistake. The
// counter, not ConnCount, is the assertion: a connection dropped without an unbind also ends at zero.
func TestConnectorUnbindsOnDrain(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{})
	rrec := otelrec.New(t)

	svc := connectorpool.New(connectorpool.Deps{
		Consumer: blockingConsumer{},
		CDR:      &fakeCDR{},
		Producer: newRecordingProducer(),
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
		Logger: slog.New(slog.NewTextHandler(discard{}, nil)),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	if !waitFor(3*time.Second, func() bool { return svc.BindReady(context.Background()) == nil }) {
		t.Fatal("bind not ready in time — the control failed")
	}
	if got := smsc.Unbinds(); got != 0 {
		t.Fatalf("SMSC saw %d unbinds before the drain, want 0", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run on drain = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pool did not stop after cancellation")
	}

	if got := smsc.Unbinds(); got == 0 {
		t.Error("the SMSC received no unbind: the draining pool dropped its bind instead of taking " +
			"leave. The SMSC then holds a half-open session until its own timeout, and the operator " +
			"sees an unexplained disconnect on every rolling deploy (guide de codage §5 [MUST])")
	}
}

// discard is an io.Writer sink; the pool logs its bind lifecycle and this test asserts on the SMSC.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
