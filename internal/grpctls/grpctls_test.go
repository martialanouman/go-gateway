package grpctls_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/grpctls"
	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
	"github.com/martialanouman/go-gateway/internal/testutil/grpctest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// serve starts a server for cfg and returns its address. It registers no service: the probe calls a
// method nobody implements, so Unimplemented means the transport let the call through.
func serve(t *testing.T, cfg config.TLS) string {
	t.Helper()
	srv, err := grpctls.NewServer(cfg, discardLogger())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return grpctest.Serve(t, srv)
}

// dialTo verifies serverName rather than the address, which these tests must pin: they reach a loopback
// port, and no certificate of ours carries an address.
func dialTo(t *testing.T, cfg config.TLS, addr, serverName string) *grpc.ClientConn {
	t.Helper()
	d, err := grpctls.Dialer(cfg, discardLogger(), serverName)
	if err != nil {
		t.Fatalf("Dialer: %v", err)
	}
	conn, err := d(addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// keep registers a connection the test built itself, for the two cases that cannot go through Dialer.
func keep(t *testing.T, conn *grpc.ClientConn, err error) *grpc.ClientConn {
	t.Helper()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// tlsConfig is the enabled configuration of a pod holding the certificate name signed by ca.
func tlsConfig(t *testing.T, ca *tlstest.CA, name string, allowed ...string) config.TLS {
	t.Helper()
	certFile, keyFile := ca.Issue(t, name, name)
	return config.TLS{
		Enabled:        true,
		CertFile:       certFile,
		KeyFile:        keyFile,
		ClientCAFile:   ca.CAFile,
		AllowedClients: allowed,
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestAMutuallyAuthenticatedCallReachesTheServer(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)
	addr := serve(t, tlsConfig(t, ca, "billing-svc"))

	conn := dialTo(t, tlsConfig(t, ca, "router-svc"), addr, "billing-svc")
	if code, err := grpctest.Probe(t, conn); code != codes.Unimplemented {
		t.Fatalf("mutually authenticated call answered %s (%v), want Unimplemented", code, err)
	}

	// Without this half the test passes on a server serving no TLS at all.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	plain := keep(t, conn, err)
	if code, err := grpctest.Probe(t, plain); code == codes.Unimplemented {
		t.Fatalf("an insecure client reached the server (%s, %v): it is not serving TLS", code, err)
	}
}

// Seven of the eight clients dial a Service by name and pin nothing, so the name verified is the
// authority of the address. It cannot go through Dialer, which takes no extra dial option: the
// connection needs a context dialler to send the bytes to the loopback listener whatever the name, and
// passthrough to keep the resolver from looking up a host that does not exist.
func TestTheNameDialledIsTheNameVerified(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)
	addr := serve(t, tlsConfig(t, ca, "billing-svc"))

	client := tlsConfig(t, ca, "router-svc")
	conf, err := tlsconf.Files{Cert: client.CertFile, Key: client.KeyFile, ClientCA: client.ClientCAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	creds := grpc.WithTransportCredentials(credentials.NewTLS(conf))
	redirect := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	})

	rightConn, err := grpc.NewClient("passthrough:///billing-svc:7001", creds, redirect)
	right := keep(t, rightConn, err)
	if code, err := grpctest.Probe(t, right); code != codes.Unimplemented {
		t.Fatalf("the name the certificate carries answered %s (%v), want Unimplemented", code, err)
	}

	// Without this half the test would pass on a client that verified nothing.
	wrongConn, err := grpc.NewClient("passthrough:///session-manager-svc:7000", creds, redirect)
	wrong := keep(t, wrongConn, err)
	code, err := grpctest.Probe(t, wrong)
	if code == codes.Unimplemented {
		t.Fatal("a peer whose certificate names somebody else was accepted")
	}
	if err == nil || !strings.Contains(err.Error(), "session-manager-svc") {
		t.Fatalf("refusal does not name the identity expected: %v", err)
	}
}

func TestAClientWithoutACertificateIsRefused(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)
	addr := serve(t, tlsConfig(t, ca, "billing-svc"))

	// The control: the same server, same instant, answers a peer holding a certificate. Without it a
	// refusal proves nothing — a server that never started refuses everything too.
	control := dialTo(t, tlsConfig(t, ca, "router-svc"), addr, "billing-svc")
	if code, err := grpctest.Probe(t, control); code != codes.Unimplemented {
		t.Fatalf("the control call answered %s (%v), want Unimplemented", code, err)
	}

	// Trusts our CA, presents nothing of its own.
	//
	// The message is NOT asserted, deliberately: under TLS 1.3 the client sends its certificate after the
	// server's Finished, so it considers the handshake done and learns of the rejection later. It reads
	// either the alert or a broken pipe depending on which wins, and pinning one of the two is a flake.
	naked := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: poolOf(t, ca), ServerName: "billing-svc"})
	nakedConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(naked))
	conn := keep(t, nakedConn, err)
	if code, err := grpctest.Probe(t, conn); code == codes.Unimplemented {
		t.Fatalf("a client with no certificate reached the server (%s, %v)", code, err)
	}
}

// Named for what it proves. Under TLS 1.3 the server's certificate arrives first, so a dialler trusting
// only its own CA aborts before ever presenting anything — the refusal is the CLIENT's, and the server's
// ClientCAs never comes into it. That direction is TestAServerRefusesAClientFromAnotherAuthority.
func TestTheDialerRefusesAServerFromAnotherAuthority(t *testing.T) {
	t.Parallel()
	ours, theirs := tlstest.NewCA(t), tlstest.NewCA(t)
	addr := serve(t, tlsConfig(t, ours, "billing-svc"))

	code, err := grpctest.Probe(t, dialTo(t, tlsConfig(t, theirs, "router-svc"), addr, "billing-svc"))
	if code == codes.Unimplemented {
		t.Fatal("a server from another authority was accepted")
	}
	// Deterministic, unlike a client-certificate refusal: this one is decided during the dialler's own
	// handshake, before it answers anything.
	if err == nil || !strings.Contains(err.Error(), "unknown authority") {
		t.Fatalf("refusal not attributed to the authority: %v", err)
	}
}

// The other direction, and the one the servers with no allowlist rest on entirely: a peer holding a
// certificate nobody we trust signed. It trusts OUR authority, so it presents its certificate and the
// refusal can only come from the server's ClientCAs.
func TestAServerRefusesAClientFromAnotherAuthority(t *testing.T) {
	t.Parallel()
	ours, theirs := tlstest.NewCA(t), tlstest.NewCA(t)
	addr := serve(t, tlsConfig(t, ours, "billing-svc"))

	control := dialTo(t, tlsConfig(t, ours, "router-svc"), addr, "billing-svc")
	if code, err := grpctest.Probe(t, control); code != codes.Unimplemented {
		t.Fatalf("the control call answered %s (%v), want Unimplemented", code, err)
	}

	// Self-signed by an authority of its own, trusting ours. The message is not asserted for the reason
	// given in TestAClientWithoutACertificateIsRefused.
	intruderCert, intruderKey := theirs.Issue(t, "router-svc", "router-svc")
	intruder := config.TLS{
		Enabled:      true,
		CertFile:     intruderCert,
		KeyFile:      intruderKey,
		ClientCAFile: ours.CAFile,
	}
	if code, err := grpctest.Probe(t, dialTo(t, intruder, addr, "billing-svc")); code == codes.Unimplemented {
		t.Fatalf("a client no authority of ours signed reached the server (%s, %v)", code, err)
	}
}

func TestTheAllowlistRefusesACallerItDoesNotName(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)

	// A certificate of our CA proves a peer is one of our pods, never WHICH one.
	addr := serve(t, tlsConfig(t, ca, "content-key-svc", "router-svc", "admin-api-svc"))

	named := dialTo(t, tlsConfig(t, ca, "router-svc"), addr, "content-key-svc")
	if code, err := grpctest.Probe(t, named); code != codes.Unimplemented {
		t.Fatalf("named caller answered %s (%v), want Unimplemented", code, err)
	}

	// Same CA, a name the server does not list. Without the allowlist this one passes. The named caller
	// above is the control; the rejection message is not asserted for the same reason as elsewhere.
	stranger := dialTo(t, tlsConfig(t, ca, "connector-pool-svc"), addr, "content-key-svc")
	if code, err := grpctest.Probe(t, stranger); code == codes.Unimplemented {
		t.Fatalf("a caller the allowlist does not name reached the server (%s, %v)", code, err)
	}
}

func TestDisabledTLSLeavesBothSidesInPlaintext(t *testing.T) {
	t.Parallel()

	// Off keeps the test suites running without certificates, on both sides of the same switch.
	addr := serve(t, config.TLS{})
	if code, err := grpctest.Probe(t, dialTo(t, config.TLS{}, addr, "")); code != codes.Unimplemented {
		t.Fatalf("plaintext call answered %s (%v), want Unimplemented", code, err)
	}
}

func TestAnUnreadableIdentityFailsAtBoot(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)

	cfg := tlsConfig(t, ca, "billing-svc")
	cfg.CertFile += ".missing"

	// A boot error, not a handshake failing later.
	if _, err := grpctls.NewServer(cfg, discardLogger()); err == nil {
		t.Error("NewServer accepted a certificate path that does not exist")
	}
	if _, err := grpctls.Dialer(cfg, discardLogger(), ""); err == nil {
		t.Error("Dialer accepted a certificate path that does not exist")
	}
}

func poolOf(t *testing.T, ca *tlstest.CA) *x509.CertPool {
	t.Helper()
	blob, err := os.ReadFile(ca.CAFile)
	if err != nil {
		t.Fatalf("read the CA: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(blob) {
		t.Fatal("the CA file holds no certificate")
	}
	return pool
}
