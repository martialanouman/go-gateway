package connectorpool

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// smscTLS issues one authority and returns the two sides of a mutual link: the config the fake SMSC
// listens with, and the one the pool dials with. The SMSC DEMANDS a client certificate, so a dial that
// presented none would fail the handshake — which is what makes this a proof of mTLS and not of a
// tunnel.
func smscTLS(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	ca := tlstest.NewCA(t)

	smscCert, smscKey := ca.Issue(t, "smsc", "smsc")
	server, err := tlsconf.Files{Cert: smscCert, Key: smscKey, ClientCA: ca.CAFile}.
		ServerConfig(tlsconf.ServerOptions{})
	if err != nil {
		t.Fatalf("smsc ServerConfig: %v", err)
	}

	poolCert, poolKey := ca.Issue(t, "connector-pool-svc", "connector-pool-svc")
	client, err = tlsconf.Files{Cert: poolCert, Key: poolKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("pool ClientConfig: %v", err)
	}
	client.ServerName = "smsc"
	return server, client
}

func bindOnce(t *testing.T, addr string, conf *tls.Config) error {
	t.Helper()
	cfg := BindConfig{
		Addr: addr, SystemID: "esme", Password: "pw",
		DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
		EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		TLS: conf,
	}
	b, err := dialAndBind(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(context.Context, *smpp.DeliverSM) error { return nil })
	if b != nil {
		b.Close()
	}
	return err
}

func TestTheOutboundBindDialsAnSMSCInTLS(t *testing.T) {
	server, client := smscTLS(t)
	smsc := fakesmsc.Start(t, fakesmsc.Config{TLSConfig: server})

	if err := bindOnce(t, smsc.Addr(), client); err != nil {
		t.Fatalf("dialAndBind over TLS: %v", err)
	}
}

func TestAPlaintextDialIsRefusedByATLSSMSC(t *testing.T) {
	server, _ := smscTLS(t)
	smsc := fakesmsc.Start(t, fakesmsc.Config{TLSConfig: server})

	if err := bindOnce(t, smsc.Addr(), nil); err == nil {
		t.Fatal("a plaintext dial bound to a TLS-only SMSC")
	}
}

func TestTheOutboundBindStaysPlaintextWithoutATLSConfig(t *testing.T) {
	smsc := fakesmsc.Start(t, fakesmsc.Config{})

	if err := bindOnce(t, smsc.Addr(), nil); err != nil {
		t.Fatalf("dialAndBind in plaintext: %v", err)
	}
}
