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
	"github.com/martialanouman/go-gateway/internal/testutil/grpctest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// serveWith starts a gRPC server carrying opt and returns its address. It registers no service: the
// probe calls a method nobody implements, so Unimplemented means the transport let the call through.
func serveWith(t *testing.T, opt grpc.ServerOption) string {
	t.Helper()
	return grpctest.Serve(t, grpc.NewServer(opt))
}

func dial(t *testing.T, addr string, opt grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, opt)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
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

	opt, err := grpctls.ServerOption(tlsConfig(t, ca, "billing-svc"), discardLogger())
	if err != nil {
		t.Fatalf("ServerOption: %v", err)
	}
	addr := serveWith(t, opt)

	dialOpt, err := grpctls.DialOptionTo(tlsConfig(t, ca, "router-svc"), discardLogger(), "billing-svc")
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}

	if code, err := grpctest.Probe(t, dial(t, addr, dialOpt)); code != codes.Unimplemented {
		t.Fatalf("mutually authenticated call answered %s (%v), want Unimplemented", code, err)
	}

	// Without this half the test passes on a server serving no TLS at all.
	plain := dial(t, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if code, err := grpctest.Probe(t, plain); code == codes.Unimplemented {
		t.Fatalf("an insecure client reached the server (%s, %v): it is not serving TLS", code, err)
	}
}

func TestTheNameDialledIsTheNameVerified(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)

	opt, err := grpctls.ServerOption(tlsConfig(t, ca, "billing-svc"), discardLogger())
	if err != nil {
		t.Fatalf("ServerOption: %v", err)
	}
	addr := serveWith(t, opt)

	// Seven of the eight clients dial a Service by name and declare no identity. The dialer sends the
	// bytes to the loopback listener whatever the name, so the name is all that is under test, and
	// passthrough keeps the resolver from looking up a host that does not exist.
	dialOpt, err := grpctls.DialOption(tlsConfig(t, ca, "router-svc"), discardLogger())
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	redirect := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	})

	right, err := grpc.NewClient("passthrough:///billing-svc:7001", dialOpt, redirect)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = right.Close() })
	if code, err := grpctest.Probe(t, right); code != codes.Unimplemented {
		t.Fatalf("the name the certificate carries answered %s (%v), want Unimplemented", code, err)
	}

	// Without this half the test would pass on a client that verified nothing.
	wrong, err := grpc.NewClient("passthrough:///session-manager-svc:7000", dialOpt, redirect)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = wrong.Close() })
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

	opt, err := grpctls.ServerOption(tlsConfig(t, ca, "billing-svc"), discardLogger())
	if err != nil {
		t.Fatalf("ServerOption: %v", err)
	}
	addr := serveWith(t, opt)

	// The control: the same server, same instant, answers a peer holding a certificate. Without it a
	// refusal proves nothing — a server that never started refuses everything too.
	proper, err := grpctls.DialOptionTo(tlsConfig(t, ca, "router-svc"), discardLogger(), "billing-svc")
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	if code, err := grpctest.Probe(t, dial(t, addr, proper)); code != codes.Unimplemented {
		t.Fatalf("the control call answered %s (%v), want Unimplemented", code, err)
	}

	// Trusts our CA, presents nothing of its own.
	//
	// The message is NOT asserted, deliberately: under TLS 1.3 the client sends its certificate after
	// the server's Finished, so it considers the handshake done and learns of the rejection later. It
	// reads either the alert or a broken pipe depending on which wins, and pinning one of the two is a
	// flake. The control above is what makes this refusal attributable.
	naked := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: poolOf(t, ca), ServerName: "billing-svc"})
	if code, err := grpctest.Probe(t, dial(t, addr, grpc.WithTransportCredentials(naked))); code == codes.Unimplemented {
		t.Fatalf("a client with no certificate reached the server (%s, %v)", code, err)
	}
}

// Named for what it proves. Under TLS 1.3 the server's certificate arrives first, so a dialler trusting
// only its own CA aborts before ever presenting anything — the refusal is the CLIENT's, and the server's
// ClientCAs never comes into it. That direction is TestAServerRefusesAClientFromAnotherAuthority.
func TestTheDialerRefusesAServerFromAnotherAuthority(t *testing.T) {
	t.Parallel()
	ours, theirs := tlstest.NewCA(t), tlstest.NewCA(t)

	opt, err := grpctls.ServerOption(tlsConfig(t, ours, "billing-svc"), discardLogger())
	if err != nil {
		t.Fatalf("ServerOption: %v", err)
	}
	addr := serveWith(t, opt)

	dialOpt, err := grpctls.DialOptionTo(tlsConfig(t, theirs, "router-svc"), discardLogger(), "billing-svc")
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	code, err := grpctest.Probe(t, dial(t, addr, dialOpt))
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

	opt, err := grpctls.ServerOption(tlsConfig(t, ours, "billing-svc"), discardLogger())
	if err != nil {
		t.Fatalf("ServerOption: %v", err)
	}
	addr := serveWith(t, opt)

	proper, err := grpctls.DialOptionTo(tlsConfig(t, ours, "router-svc"), discardLogger(), "billing-svc")
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	if code, err := grpctest.Probe(t, dial(t, addr, proper)); code != codes.Unimplemented {
		t.Fatalf("the control call answered %s (%v), want Unimplemented", code, err)
	}

	// Self-signed by an authority of its own, trusting ours. The message is not asserted for the reason
	// given in TestAClientWithoutACertificateIsRefused.
	intruderCert, intruderKey := theirs.Issue(t, "router-svc", "router-svc")
	intruder, err := grpctls.DialOptionTo(config.TLS{
		Enabled:      true,
		CertFile:     intruderCert,
		KeyFile:      intruderKey,
		ClientCAFile: ours.CAFile,
	}, discardLogger(), "billing-svc")
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	if code, err := grpctest.Probe(t, dial(t, addr, intruder)); code == codes.Unimplemented {
		t.Fatalf("a client no authority of ours signed reached the server (%s, %v)", code, err)
	}
}

func TestTheAllowlistRefusesACallerItDoesNotName(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)

	// A certificate of our CA proves a peer is one of our pods, never WHICH one.
	opt, err := grpctls.ServerOption(tlsConfig(t, ca, "content-key-svc", "router-svc", "admin-api-svc"), discardLogger())
	if err != nil {
		t.Fatalf("ServerOption: %v", err)
	}
	addr := serveWith(t, opt)

	named, err := grpctls.DialOptionTo(tlsConfig(t, ca, "router-svc"), discardLogger(), "content-key-svc")
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	if code, err := grpctest.Probe(t, dial(t, addr, named)); code != codes.Unimplemented {
		t.Fatalf("named caller answered %s (%v), want Unimplemented", code, err)
	}

	// Same CA, a name the server does not list. Without the allowlist this one passes.
	stranger, err := grpctls.DialOptionTo(tlsConfig(t, ca, "connector-pool-svc"), discardLogger(), "content-key-svc")
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	// The named caller above is the control; the rejection message is not asserted for the same reason
	// as in TestAClientWithoutACertificateIsRefused.
	if code, err := grpctest.Probe(t, dial(t, addr, stranger)); code == codes.Unimplemented {
		t.Fatalf("a caller the allowlist does not name reached the server (%s, %v)", code, err)
	}
}

func TestDisabledTLSLeavesBothSidesInPlaintext(t *testing.T) {
	t.Parallel()

	// Off keeps the test suites running without certificates, on both sides of the same switch.
	opt, err := grpctls.ServerOption(config.TLS{}, discardLogger())
	if err != nil {
		t.Fatalf("ServerOption: %v", err)
	}
	addr := serveWith(t, opt)

	dialOpt, err := grpctls.DialOption(config.TLS{}, discardLogger())
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	if code, err := grpctest.Probe(t, dial(t, addr, dialOpt)); code != codes.Unimplemented {
		t.Fatalf("plaintext call answered %s (%v), want Unimplemented", code, err)
	}
}

func TestAnUnreadableIdentityFailsAtBoot(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)

	cfg := tlsConfig(t, ca, "billing-svc")
	cfg.CertFile += ".missing"

	// A boot error, not a handshake failing later.
	if _, err := grpctls.ServerOption(cfg, discardLogger()); err == nil {
		t.Error("ServerOption accepted a certificate path that does not exist")
	}
	if _, err := grpctls.DialOption(cfg, discardLogger()); err == nil {
		t.Error("DialOption accepted a certificate path that does not exist")
	}
}

func poolOf(t *testing.T, ca *tlstest.CA) *x509.CertPool {
	t.Helper()
	pem, err := os.ReadFile(ca.CAFile)
	if err != nil {
		t.Fatalf("read the CA: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("the CA file holds no certificate")
	}
	return pool
}
