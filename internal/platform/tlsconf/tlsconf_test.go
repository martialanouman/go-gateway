package tlsconf_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"os"
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

// TestServerRefusesAClientOutsideTheAllowlist covers the half that a tunnel alone does not: a
// certificate from our CA proves the peer is one of our pods, never WHICH one. content-key-svc hands
// out the data key of a customer; without this check, any pod of the cluster could ask for any key.
func TestServerRefusesAClientOutsideTheAllowlist(t *testing.T) {
	ca := tlstest.NewCA(t)
	serverCert, serverKey := ca.Issue(t, "server", "content-key-svc")
	strangerCert, strangerKey := ca.Issue(t, "stranger", "config-sync")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.
		ServerConfig([]string{"router-svc", "admin-api-svc"})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	clientCfg, err := tlsconf.Files{Cert: strangerCert, Key: strangerKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"

	if _, err := exchange(serve(t, serverCfg), clientCfg); err == nil {
		t.Fatal("a client of our CA was served although its SAN is not on the allowlist")
	}
}

// TestServerServesAClientOnTheAllowlist is the other half, and it is what makes the test above mean
// something: a guard that refuses everyone would pass it.
func TestServerServesAClientOnTheAllowlist(t *testing.T) {
	ca := tlstest.NewCA(t)
	serverCert, serverKey := ca.Issue(t, "server", "content-key-svc")
	clientCert, clientKey := ca.Issue(t, "client", "router-svc")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.
		ServerConfig([]string{"router-svc", "admin-api-svc"})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	clientCfg, err := tlsconf.Files{Cert: clientCert, Key: clientKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"

	if _, err := exchange(serve(t, serverCfg), clientCfg); err != nil {
		t.Fatalf("exchange: %v, want the allowlisted client to be served", err)
	}
}

// TestTheAllowlistRefusalNamesThePresentedSAN: a refused mTLS handshake is otherwise undebuggable. The
// client is told nothing useful (TLS says "bad certificate" and stops), so the server side must say
// which identity it saw and which it expected. None of it is a secret.
func TestTheAllowlistRefusalNamesThePresentedSAN(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.Issue(t, "peer", "config-sync")

	cfg, err := tlsconf.Files{Cert: certFile, Key: keyFile, ClientCA: ca.CAFile}.
		ServerConfig([]string{"router-svc"})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	// Reach the verifier the way a handshake does, without a socket: the config it hands out per
	// handshake carries the callback under test.
	perHandshake, err := cfg.GetConfigForClient(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetConfigForClient: %v", err)
	}
	leaf := readLeaf(t, certFile)
	err = perHandshake.VerifyConnection(tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}})
	if err == nil {
		t.Fatal("VerifyConnection accepted a SAN outside the allowlist")
	}
	if !strings.Contains(err.Error(), "config-sync") || !strings.Contains(err.Error(), "router-svc") {
		t.Errorf("error = %q, want it to name the presented SAN and the allowed ones", err)
	}
}

// readLeaf parses a PEM certificate file into the x509 form a verified chain carries.
func readLeaf(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatalf("%s holds no PEM block", path)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return cert
}

// TestTheAllowlistReadsTheSANAndNotTheCommonName pins which field carries the identity. The CN is
// deprecated for that, cert-manager fills dnsNames from a Certificate's spec, and a check that read the
// CN instead would admit a certificate whose SANs say something else entirely.
func TestTheAllowlistReadsTheSANAndNotTheCommonName(t *testing.T) {
	ca := tlstest.NewCA(t)
	// CN says router-svc, the SAN says config-sync. The allowlist admits router-svc.
	certFile, keyFile := ca.Issue(t, "router-svc", "config-sync")

	cfg, err := tlsconf.Files{Cert: certFile, Key: keyFile, ClientCA: ca.CAFile}.
		ServerConfig([]string{"router-svc"})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	perHandshake, err := cfg.GetConfigForClient(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetConfigForClient: %v", err)
	}

	leaf := readLeaf(t, certFile)
	if leaf.Subject.CommonName != "router-svc" {
		t.Fatalf("fixture CN = %q, want router-svc: the test cannot tell CN from SAN otherwise",
			leaf.Subject.CommonName)
	}
	err = perHandshake.VerifyConnection(tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}})
	if err == nil {
		t.Fatal("a certificate whose COMMON NAME is allowed, but whose SAN is not, was accepted")
	}
}
