package tlsconf_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.ServerConfig(tlsconf.ServerOptions{})
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

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.ServerConfig(tlsconf.ServerOptions{})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}

	// A client that trusts our CA but carries no certificate of its own.
	pool, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	bare := &tls.Config{RootCAs: pool.RootCAs, ServerName: "content-key-svc", MinVersion: tls.VersionTLS13}

	_, err = exchange(serve(t, serverCfg), bare)
	if err == nil {
		t.Fatal("a client presenting no certificate was served")
	}
	// Without naming the reason, a crashed server or a plain EOF would pass for a refusal.
	if !strings.Contains(err.Error(), "certificate required") {
		t.Errorf("error = %v, want the TLS 1.3 \"certificate required\" alert", err)
	}
}

// TestServerRefusesAClientFromAnotherCA: trusting any certificate, rather than ours, would make the
// whole exercise decorative.
func TestServerRefusesAClientFromAnotherCA(t *testing.T) {
	ours, theirs := tlstest.NewCA(t), tlstest.NewCA(t)
	serverCert, serverKey := ours.Issue(t, "server", "content-key-svc")
	intruderCert, intruderKey := theirs.Issue(t, "intruder", "router-svc")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ours.CAFile}.ServerConfig(tlsconf.ServerOptions{})
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
	// "a TLS alert" would also cover handshake failure or internal error; the alert that says the chain
	// did not verify is the one this test is about.
	if !strings.Contains(err.Error(), "unknown certificate authority") &&
		!strings.Contains(err.Error(), "bad certificate") {
		t.Errorf("error = %v, want the alert of a chain that does not verify", err)
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
		ServerConfig(tlsconf.ServerOptions{AllowedClients: []string{"router-svc", "admin-api-svc"}})
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
		ServerConfig(tlsconf.ServerOptions{AllowedClients: []string{"router-svc", "admin-api-svc"}})
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
		ServerConfig(tlsconf.ServerOptions{AllowedClients: []string{"router-svc"}})
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
		ServerConfig(tlsconf.ServerOptions{AllowedClients: []string{"router-svc"}})
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

// peerSerial dials and returns the serial of the certificate the server presented. The serial is what
// distinguishes two generations of the same identity: a rotation keeps the paths and the SANs.
func peerSerial(t *testing.T, addr net.Addr, cfg *tls.Config) string {
	t.Helper()
	conn, err := tls.Dial("tcp", addr.String(), cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	return conn.ConnectionState().PeerCertificates[0].SerialNumber.String()
}

// serveForever accepts connections until the test ends, so a rotation can be observed across two
// handshakes against one listener — which is the case that matters: the process does not restart.
func serveForever(t *testing.T, cfg *tls.Config) net.Addr {
	t.Helper()
	lis, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return lis.Addr()
}

// TestTheServerServesARotatedCertificateWithoutRestarting is the whole reason the files are read per
// handshake. cert-manager renews a leaf about every sixty days and the kubelet rewrites the volume in
// place; a process that cached its certificate at boot would serve an expired one until someone noticed.
func TestTheServerServesARotatedCertificateWithoutRestarting(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.Issue(t, "server", "content-key-svc")
	clientCert, clientKey := ca.Issue(t, "client", "router-svc")

	serverCfg, err := tlsconf.Files{Cert: certFile, Key: keyFile, ClientCA: ca.CAFile}.ServerConfig(tlsconf.ServerOptions{})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	clientCfg, err := tlsconf.Files{Cert: clientCert, Key: clientKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"
	// A resumed session skips the certificate exchange, which would hide the rotation behind a ticket.
	clientCfg.ClientSessionCache = nil

	addr := serveForever(t, serverCfg)
	before := peerSerial(t, addr, clientCfg)

	// The rotation: same paths, new bytes. The timestamps are set explicitly because two writes can land
	// in the same clock tick, and a cache that missed the change would then look correct here.
	ca.IssueInto(t, certFile, keyFile, "content-key-svc")
	future := time.Now().Add(time.Second)
	for _, p := range []string{certFile, keyFile} {
		if err := os.Chtimes(p, future, future); err != nil {
			t.Fatalf("chtimes %s: %v", p, err)
		}
	}

	if after := peerSerial(t, addr, clientCfg); after == before {
		t.Errorf("serial after rotation = %s, want a different certificate: the listener is still "+
			"serving the one it read at boot", after)
	}
}

// TestAChangedCAIsAnnouncedBecauseItNeedsARestart covers the failure mode of the limit step-300 accepts:
// the dialling side cannot rotate its root pool, so a CA change needs the clients restarted. Left
// unsaid, that shows up as x509: unknown authority on a pod nobody touched. The loader already reads the
// file at every handshake, so the whole guard is one comparison.
func TestAChangedCAIsAnnouncedBecauseItNeedsARestart(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.Issue(t, "client", "router-svc")

	var logged bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))

	files := tlsconf.Files{Cert: certFile, Key: keyFile, ClientCA: ca.CAFile, Logger: logger}
	cfg, err := files.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	if _, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{}); err != nil {
		t.Fatalf("GetClientCertificate: %v", err)
	}
	if logged.Len() != 0 {
		t.Fatalf("a steady CA logged %q", logged.String())
	}

	// A second authority overwrites the ca.crt the loader read at boot.
	other := tlstest.NewCA(t)
	raw, err := os.ReadFile(other.CAFile)
	if err != nil {
		t.Fatalf("read the other CA: %v", err)
	}
	if err := os.WriteFile(ca.CAFile, raw, 0o600); err != nil {
		t.Fatalf("overwrite the CA: %v", err)
	}
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(ca.CAFile, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if _, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{}); err != nil {
		t.Fatalf("GetClientCertificate after the CA change: %v", err)
	}
	if !strings.Contains(logged.String(), "restart") {
		t.Errorf("logs = %q, want a warning naming the restart a CA change requires", logged.String())
	}
}

// TestTheClientRefusesAServerFromAnotherCA is the half nothing covered: every other test here aims at
// what the SERVER accepts, so the client's own verification was exercised by nothing. Adding
// InsecureSkipVerify to ClientConfig used to leave the whole suite green — which is precisely what the
// design forbids, and an attacker on the path is all it takes.
func TestTheClientRefusesAServerFromAnotherCA(t *testing.T) {
	ours, theirs := tlstest.NewCA(t), tlstest.NewCA(t)
	clientCert, clientKey := ours.Issue(t, "client", "router-svc")
	// An impostor: it serves content-key-svc's name, signed by an authority we do not trust.
	impostorCert, impostorKey := theirs.Issue(t, "impostor", "content-key-svc")

	serverCfg, err := tlsconf.Files{Cert: impostorCert, Key: impostorKey, ClientCA: theirs.CAFile}.
		ServerConfig(tlsconf.ServerOptions{})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	clientCfg, err := tlsconf.Files{Cert: clientCert, Key: clientKey, ClientCA: ours.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"

	_, err = exchange(serve(t, serverCfg), clientCfg)
	if err == nil {
		t.Fatal("the client accepted a server signed by an authority it does not trust")
	}
	if !strings.Contains(err.Error(), "unknown authority") &&
		!strings.Contains(err.Error(), "certificate signed by") {
		t.Errorf("error = %v, want the client's own chain verification to refuse it", err)
	}
}

// TestTheServerAcceptsAClientOfARotatedCA is the reason ServerConfig goes through GetConfigForClient at
// all. ClientCAs is read once at the start of a handshake and no callback refreshes it, so a pool built
// at boot would never move — and a CA rotation would then need every server restarted, which the design
// says it does not.
func TestTheServerAcceptsAClientOfARotatedCA(t *testing.T) {
	first, second := tlstest.NewCA(t), tlstest.NewCA(t)
	serverCert, serverKey := first.Issue(t, "server", "content-key-svc")
	newClientCert, newClientKey := second.Issue(t, "client", "router-svc")

	// The client keeps its own copy of the first authority: the server's leaf is not rotated here, so
	// the client must still be able to verify it after the server's ca.crt has moved on.
	firstCAForClient := filepath.Join(t.TempDir(), "old-ca.crt")
	original, err := os.ReadFile(first.CAFile)
	if err != nil {
		t.Fatalf("read the first CA: %v", err)
	}
	if err := os.WriteFile(firstCAForClient, original, 0o600); err != nil {
		t.Fatalf("copy the first CA: %v", err)
	}

	caFile := first.CAFile
	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: caFile}.
		ServerConfig(tlsconf.ServerOptions{})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	addr := serveForever(t, serverCfg)

	// The rotation: the authority file now holds the SECOND CA. The server's own leaf is untouched.
	raw, err := os.ReadFile(second.CAFile)
	if err != nil {
		t.Fatalf("read the second CA: %v", err)
	}
	if err := os.WriteFile(caFile, raw, 0o600); err != nil {
		t.Fatalf("overwrite the CA: %v", err)
	}
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(caFile, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// This client could not have been accepted a moment ago: its authority did not exist in the pool.
	clientCfg, err := tlsconf.Files{Cert: newClientCert, Key: newClientKey, ClientCA: firstCAForClient}.
		ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"
	if _, err := exchange(addr, clientCfg); err != nil {
		t.Fatalf("exchange: %v, want the listener to verify against the rotated authority", err)
	}
}

// TestTheCacheNoticesAChangeThatKeepsTheTimestamp covers the half of the cache key the rotation tests
// cannot reach: they force the modification time, so a cache keyed on the time alone would pass them.
// Two writes inside one clock tick are the case this guards, and the size is what catches them.
func TestTheCacheNoticesAChangeThatKeepsTheTimestamp(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.Issue(t, "server", "content-key-svc")
	clientCert, clientKey := ca.Issue(t, "client", "router-svc")

	// Each file keeps ITS OWN timestamp: restoring the certificate's onto the key would move the key's,
	// and the cache would then notice the change through the key rather than through the size.
	stamps := map[string]os.FileInfo{}
	for _, f := range []string{certFile, keyFile} {
		fi, err := os.Stat(f)
		if err != nil {
			t.Fatalf("stat %s: %v", f, err)
		}
		stamps[f] = fi
	}
	before := stamps[certFile]

	serverCfg, err := tlsconf.Files{Cert: certFile, Key: keyFile, ClientCA: ca.CAFile}.
		ServerConfig(tlsconf.ServerOptions{})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	clientCfg, err := tlsconf.Files{Cert: clientCert, Key: clientKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"

	addr := serveForever(t, serverCfg)
	first := peerSerial(t, addr, clientCfg)

	// A longer SAN list makes a longer certificate, then the timestamps are put back exactly where they
	// were: only the size differs.
	ca.IssueInto(t, certFile, keyFile, "content-key-svc", "content-key-svc.gateway.svc.cluster.local")
	for f, fi := range stamps {
		if err := os.Chtimes(f, fi.ModTime(), fi.ModTime()); err != nil {
			t.Fatalf("chtimes %s: %v", f, err)
		}
	}
	after, err := os.Stat(certFile)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if after.Size() == before.Size() {
		t.Fatalf("the reissued certificate has the same size (%d): this test can prove nothing", after.Size())
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("modification time moved (%s -> %s): the test is no longer about the size alone",
			before.ModTime(), after.ModTime())
	}

	if got := peerSerial(t, addr, clientCfg); got == first {
		t.Error("the listener kept serving the old certificate: a change that preserves the timestamp " +
			"is invisible to a cache keyed on the time alone")
	}
}

// TestTheHandshakeFloorIsTLS13 pins the internal floor. Everything this package configures today is
// pod-to-pod, where there is no old client to spare; the public REST API of step-300c is the exception
// and will say so in its own constructor.
func TestTheHandshakeFloorIsTLS13(t *testing.T) {
	ca := tlstest.NewCA(t)
	serverCert, serverKey := ca.Issue(t, "server", "content-key-svc")
	clientCert, clientKey := ca.Issue(t, "client", "router-svc")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.
		ServerConfig(tlsconf.ServerOptions{})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	// Built by hand, not through ClientConfig: that one has a 1.3 floor of its own, and the handshake
	// would then fail on the CLIENT before the server ever had to refuse anything.
	pair, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		t.Fatalf("load the client pair: %v", err)
	}
	caPEM, err := os.ReadFile(ca.CAFile)
	if err != nil {
		t.Fatalf("read the CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("the CA file holds no certificate")
	}
	legacy := &tls.Config{
		ServerName:   "content-key-svc",
		Certificates: []tls.Certificate{pair},
		RootCAs:      roots,
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
	}

	if _, err := exchange(serve(t, serverCfg), legacy); err == nil {
		t.Fatal("a TLS 1.2 client was served: the internal floor is not 1.3")
	}
}

// TestALPNReachesThePerHandshakeConfig covers a downgrade that announces itself with nothing. Neither
// net/http nor gRPC lets the outer config reach GetConfigForClient — both pass a clone — so a protocol
// list set outside is lost, negotiateALPN then returns an empty protocol WITHOUT an error, and HTTP/2 is
// simply off.
func TestALPNReachesThePerHandshakeConfig(t *testing.T) {
	ca := tlstest.NewCA(t)
	serverCert, serverKey := ca.Issue(t, "server", "content-key-svc")
	clientCert, clientKey := ca.Issue(t, "client", "router-svc")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.
		ServerConfig(tlsconf.ServerOptions{NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	clientCfg, err := tlsconf.Files{Cert: clientCert, Key: clientKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"
	clientCfg.NextProtos = []string{"h2"}

	conn, err := tls.Dial("tcp", serveForever(t, serverCfg).String(), clientCfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if got := conn.ConnectionState().NegotiatedProtocol; got != "h2" {
		t.Errorf("negotiated protocol = %q, want \"h2\": the per-handshake config dropped the ALPN list", got)
	}
}

// TestTheIdentityCheckSurvivesSessionResumption pins the hook itself. On a resumed TLS 1.3 session
// VerifyPeerCertificate is never called again, so a caller dropped from the allowlist would keep its
// access for as long as its ticket lives; VerifyConnection runs on both paths.
//
// The assertion is structural because the behavioural version cannot be written from outside: a client
// refused by the allowlist never gets a ticket in the first place, so there is no resumed handshake to
// observe it on. What IS observable is that resumption happens at all, and that a session resumed this
// way is still served — the second half of the same guarantee.
func TestTheIdentityCheckSurvivesSessionResumption(t *testing.T) {
	ca := tlstest.NewCA(t)
	serverCert, serverKey := ca.Issue(t, "server", "content-key-svc")
	clientCert, clientKey := ca.Issue(t, "client", "router-svc")

	serverCfg, err := tlsconf.Files{Cert: serverCert, Key: serverKey, ClientCA: ca.CAFile}.
		ServerConfig(tlsconf.ServerOptions{AllowedClients: []string{"router-svc"}})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	perHandshake, err := serverCfg.GetConfigForClient(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetConfigForClient: %v", err)
	}
	if perHandshake.VerifyConnection == nil {
		t.Error("VerifyConnection is nil: the identity check would be skipped on every resumed session")
	}
	if perHandshake.VerifyPeerCertificate != nil {
		t.Error("VerifyPeerCertificate is set: it is not called on a resumed session, so an allowlist " +
			"enforced there would lapse with the ticket rather than with the certificate")
	}

	clientCfg, err := tlsconf.Files{Cert: clientCert, Key: clientKey, ClientCA: ca.CAFile}.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"
	clientCfg.ClientSessionCache = tls.NewLRUClientSessionCache(4)

	addr := serveForever(t, serverCfg)
	// The first handshake collects the ticket; TLS 1.3 sends it after the handshake, so the connection
	// has to be read from before it is closed.
	for range 2 {
		conn, err := tls.Dial("tcp", addr.String(), clientCfg)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatalf("write: %v", err)
		}
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatalf("read: %v", err)
		}
		state := conn.ConnectionState()
		_ = conn.Close()
		if state.DidResume {
			return // resumed, and served: the allowlist did not lapse with the ticket
		}
	}
	t.Skip("no session was resumed; this Go runtime issues no usable ticket here")
}

// TestTheCAWarningIsEmittedOnce: the loader reads the files at EVERY handshake, so a warning without a
// latch is a warning per handshake — on a busy service that is the log drowning the thing it warns about.
func TestTheCAWarningIsEmittedOnce(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.Issue(t, "client", "router-svc")

	var logged bytes.Buffer
	files := tlsconf.Files{
		Cert: certFile, Key: keyFile, ClientCA: ca.CAFile,
		Logger: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	cfg, err := files.ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}

	other := tlstest.NewCA(t)
	raw, err := os.ReadFile(other.CAFile)
	if err != nil {
		t.Fatalf("read the other CA: %v", err)
	}
	if err := os.WriteFile(ca.CAFile, raw, 0o600); err != nil {
		t.Fatalf("overwrite the CA: %v", err)
	}
	future := time.Now().Add(time.Second)
	if err := os.Chtimes(ca.CAFile, future, future); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	if _, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{}); err != nil {
		t.Fatalf("GetClientCertificate: %v", err)
	}

	// A SECOND rotation. Repeated handshakes alone would not test the latch — the cache answers them
	// without reloading — so the file has to move again for the warning to be reachable a second time.
	third := tlstest.NewCA(t)
	raw, err = os.ReadFile(third.CAFile)
	if err != nil {
		t.Fatalf("read the third CA: %v", err)
	}
	if err := os.WriteFile(ca.CAFile, raw, 0o600); err != nil {
		t.Fatalf("overwrite the CA again: %v", err)
	}
	later := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(ca.CAFile, later, later); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
	if _, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{}); err != nil {
		t.Fatalf("GetClientCertificate after the second rotation: %v", err)
	}

	if got := strings.Count(logged.String(), "authority changed"); got != 1 {
		t.Errorf("the authority warning was logged %d times over two rotations, want 1 — the restart it "+
			"asks for is the same one either way:\n%s", got, logged.String())
	}
}

// TestAnExpiredCertificateIsAnnouncedAtLoad: tls.LoadX509KeyPair parses the leaf and checks that the key
// matches it — it never looks at NotAfter. Without this, a pod boots green, passes readiness, and every
// peer gets "certificate has expired" while this side logs a handshake failure with no cause.
func TestAnExpiredCertificateIsAnnouncedAtLoad(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.IssueExpired(t, "server", "content-key-svc")

	var logged bytes.Buffer
	files := tlsconf.Files{
		Cert: certFile, Key: keyFile, ClientCA: ca.CAFile,
		Logger: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn})),
	}
	// Loading still succeeds: refusing the boot would turn a partial outage into a CrashLoopBackOff,
	// and cert-manager may be renewing the file at that very moment.
	if _, err := files.ServerConfig(tlsconf.ServerOptions{}); err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	if !strings.Contains(logged.String(), "EXPIRED") {
		t.Errorf("logs = %q, want the expiry called out at load", logged.String())
	}
}
