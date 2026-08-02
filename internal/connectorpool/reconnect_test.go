package connectorpool_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/connector/reconnect"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/observability"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// TestLinkStatusUpThenDown: a bound pool reports link_status "up" while serving and "down" once stopped.
func TestLinkStatusUpThenDown(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{})
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Producer: discardProducer{},
		Consumer: blockingConsumer{},
		CDR:      &fakeCDR{},
		Bind:     poolBind(smsc.Addr(), 1),
		Tracer:   observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	if got := svc.LinkStatus(); got != "down" {
		t.Errorf("before start LinkStatus = %q, want down", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()

	if !waitFor(2*time.Second, func() bool { return svc.LinkStatus() == "up" }) {
		cancel()
		<-done
		t.Fatal("link never came up")
	}
	cancel()
	<-done
	if got := svc.LinkStatus(); got != "down" {
		t.Errorf("after stop LinkStatus = %q, want down", got)
	}
}

// TestReconnectStopsOnBadPassword: an ESME_RINVPASWD bind rejection stops Run immediately, even with
// auto-reconnect enabled — the loop must not hammer the SMSC with bad credentials.
func TestReconnectStopsOnBadPassword(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{RejectBind: errs.StatusInvalidPasswd})
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Producer: discardProducer{},
		Consumer: blockingConsumer{},
		CDR:      &fakeCDR{},
		Bind:     poolBind(smsc.Addr(), 1),
		Reconnect: reconnect.Config{
			Enabled: true, InitialDelay: time.Millisecond, Multiplier: 2, MaxDelay: 10 * time.Millisecond,
		},
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	done := make(chan error, 1)
	go func() { done <- svc.Run(context.Background()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run returned nil, want the bind-rejected error")
		}
		var rej *connectorpool.BindRejectedError
		if !errors.As(err, &rej) || rej.Status != errs.StatusInvalidPasswd {
			t.Errorf("Run error = %v, want a BindRejectedError with ESME_RINVPASWD", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop on bad password (it is retrying)")
	}
}

// TestReconnectDisabledDoesNotRetry: with reconnect disabled, a transient bind rejection returns at once.
func TestReconnectDisabledDoesNotRetry(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{RejectBind: errs.StatusSysErr})
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Producer: discardProducer{},
		Consumer: blockingConsumer{},
		CDR:      &fakeCDR{},
		Bind:     poolBind(smsc.Addr(), 1),
		// Reconnect zero value = disabled.
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
	})

	done := make(chan error, 1)
	go func() { done <- svc.Run(context.Background()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run returned nil, want the bind-rejected error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return (retrying despite reconnect disabled)")
	}
}
