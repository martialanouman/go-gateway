package tlsconf_test

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// serve accepts exactly one connection, reads a line and echoes it back. It is the smallest peer that
// proves a handshake completed: a TLS handshake is lazy on both sides, so a test that only dials
// proves nothing — the bytes have to cross.
func serve(t *testing.T, cfg *tls.Config) net.Addr {
	t.Helper()
	lis, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	go func() {
		conn, err := lis.Accept()
		if err != nil {
			return // the listener closed: the test is over
		}
		defer func() { _ = conn.Close() }()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		_, _ = conn.Write(buf)
	}()
	return lis.Addr()
}

// exchange dials addr with cfg and sends four bytes, returning what came back. The handshake happens on
// the first write, so its error surfaces here rather than at Dial.
func exchange(addr net.Addr, cfg *tls.Config) (string, error) {
	conn, err := tls.Dial("tcp", addr.String(), cfg)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte("ping")); err != nil {
		return "", err
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// TestMutualHandshakeSucceedsBetweenPeersOfTheSameCA is the base case: both sides present a certificate
// the other's CA signed, and the bytes cross.
func TestMutualHandshakeSucceedsBetweenPeersOfTheSameCA(t *testing.T) {
	ca := tlstest.NewCA(t)
	serverCert, serverKey := ca.Issue(t, "server", "content-key-svc")
	clientCert, clientKey := ca.Issue(t, "client", "router-svc")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.ServerConfig(nil)
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	clientCfg, err := tlsconf.Files{Cert: clientCert, Key: clientKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"

	got, err := exchange(serve(t, serverCfg), clientCfg)
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if got != "ping" {
		t.Errorf("echo = %q, want %q", got, "ping")
	}
}

// TestServerRefusesAClientWithoutACertificate: mTLS is the point. A client that presents nothing must
// not be served, or every pod of the cluster is an authorised caller.
func TestServerRefusesAClientWithoutACertificate(t *testing.T) {
	ca := tlstest.NewCA(t)
	serverCert, serverKey := ca.Issue(t, "server", "content-key-svc")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.ServerConfig(nil)
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}

	// A client that trusts our CA but carries no certificate of its own.
	pool, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	bare := &tls.Config{RootCAs: pool.RootCAs, ServerName: "content-key-svc", MinVersion: tls.VersionTLS13}

	if _, err := exchange(serve(t, serverCfg), bare); err == nil {
		t.Fatal("a client presenting no certificate was served")
	}
}

// TestServerRefusesAClientFromAnotherCA: trusting any certificate, rather than ours, would make the
// whole exercise decorative.
func TestServerRefusesAClientFromAnotherCA(t *testing.T) {
	ours, theirs := tlstest.NewCA(t), tlstest.NewCA(t)
	serverCert, serverKey := ours.Issue(t, "server", "content-key-svc")
	intruderCert, intruderKey := theirs.Issue(t, "intruder", "router-svc")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ours.CAFile}.ServerConfig(nil)
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	// The intruder trusts OUR CA as a server root — only its client certificate is foreign.
	clientCfg, err := tlsconf.Files{Cert: intruderCert, Key: intruderKey, ClientCA: ours.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"

	_, err = exchange(serve(t, serverCfg), clientCfg)
	if err == nil {
		t.Fatal("a client signed by another CA was served")
	}
	var alert tls.AlertError
	if !errors.As(err, &alert) && !strings.Contains(err.Error(), "certificate") {
		t.Errorf("error = %v, want a certificate rejection", err)
	}
}
