package connectorpool_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// fakeConfigSource returns a live pool size + reconnect policy the test can change.
type fakeConfigSource struct {
	size atomic.Int32
	rc   reconnect.Config
}

func (c *fakeConfigSource) Load(_ context.Context, _ uuid.UUID) (int, reconnect.Config, error) {
	return int(c.size.Load()), c.rc, nil
}

// fakeStatusControl exposes a settable reconfigure generation and records published link statuses.
type fakeStatusControl struct {
	gen       atomic.Int64
	published atomic.Int32
}

func (s *fakeStatusControl) PublishBind(_ context.Context, _ uuid.UUID, _ string, _ int, _ string, _ int) error {
	s.published.Add(1)
	return nil
}
func (s *fakeStatusControl) Gen(_ context.Context, _ uuid.UUID) (int64, error) {
	return s.gen.Load(), nil
}

// TestBindPoolResizesOnReconfigure is the step-128b acceptance: bumping the reconfigure generation with a
// new bind_pool_size makes the pool re-dial at the new size (1 → 4 → 4 live binds).
func TestBindPoolResizesOnReconfigure(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{})
	cfg := &fakeConfigSource{rc: reconnect.Config{Enabled: true, InitialDelay: 5 * time.Millisecond, MaxDelay: 20 * time.Millisecond}}
	cfg.size.Store(1)
	ctrl := &fakeStatusControl{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Producer:        discardProducer{},
		Consumer:        blockingConsumer{},
		CDR:             &fakeCDR{},
		Bind:            poolBind(smsc.Addr(), 1),
		ConnectorID:     uuid.New(),
		ConfigSource:    cfg,
		StatusControl:   ctrl,
		StatusHeartbeat: 10 * time.Millisecond,
		PodID:           "pod-test",
		Tracer:          observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	if !waitFor(3*time.Second, func() bool { return smsc.ConnCount() == 1 }) {
		cancel()
		<-done
		t.Fatalf("initial binds = %d, want 1", smsc.ConnCount())
	}
	// Resize to 4 and bump the generation: the status heartbeat sees the change and forces a re-dial.
	cfg.size.Store(4)
	ctrl.gen.Store(1)

	if !waitFor(5*time.Second, func() bool { return smsc.ConnCount() == 4 }) {
		cancel()
		<-done
		t.Fatalf("after resize binds = %d, want 4", smsc.ConnCount())
	}
	if ctrl.published.Load() == 0 {
		t.Error("no per-bind status was published")
	}
	cancel()
	<-done
}

// TestParkedNotExitedWhenReconnectDisabled: with auto-reconnect off but a control plane wired, a
// permanent bind rejection PARKS the pod (Run stays alive) instead of exiting — so k8s does not restart
// into a harsher reconnect loop. Cancelling the context releases it.
func TestParkedNotExitedWhenReconnectDisabled(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{RejectBind: 0x0000000E}) // ESME_RINVPASWD
	ctrl := &fakeStatusControl{}
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Producer:        discardProducer{},
		Consumer:        blockingConsumer{},
		CDR:             &fakeCDR{},
		Bind:            poolBind(smsc.Addr(), 1),
		ConnectorID:     uuid.New(),
		StatusControl:   ctrl, // wired → park instead of exit
		StatusHeartbeat: 10 * time.Millisecond,
		Tracer:          observability.Tracer(rrec.Provider(), "connector-pool"),
		// Reconnect zero value = disabled.
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	// Run must NOT return while parked (it stays alive waiting for a rebind).
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while it should be parked (staying alive)", err)
	case <-time.After(300 * time.Millisecond):
	}
	if svc.LinkStatus() != "down" {
		t.Errorf("parked LinkStatus = %q, want down", svc.LinkStatus())
	}
	// Shutdown releases the park.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("parked Run did not return after context cancel")
	}
}
