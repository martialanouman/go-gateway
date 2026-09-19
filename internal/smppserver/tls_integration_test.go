package smppserver_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"testing"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
	"github.com/martialanouman/go-gateway/internal/smpp"
	smppsession "github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

const smppServerSAN = "smpp-server-svc"

func smppTLSServer(t *testing.T) (*tls.Config, string) {
	t.Helper()
	ca := tlstest.NewCA(t)
	cert, key := ca.Issue(t, smppServerSAN, smppServerSAN)
	conf, err := tlsconf.Files{Cert: cert, Key: key, ClientCA: ca.CAFile}.PublicServerConfig(nil)
	if err != nil {
		t.Fatalf("PublicServerConfig: %v", err)
	}
	return conf, ca.CAFile
}

func esmeTLSConfig(t *testing.T, caFile string) *tls.Config {
	t.Helper()
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("read the CA: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("the CA file holds no certificate")
	}
	return &tls.Config{
		RootCAs:    roots,
		ServerName: smppServerSAN,
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS12,
	}
}

// dialTLSESME opens an SMPP session over TLS, writing proxyHeader before the handshake when it is not
// empty — the order the wire imposes, and the one this test exists to pin.
func dialTLSESME(t *testing.T, addr, proxyHeader string, conf *tls.Config) (*esme, *tls.Conn) {
	t.Helper()
	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial esme: %v", err)
	}
	if proxyHeader != "" {
		if _, err := raw.Write([]byte(proxyHeader)); err != nil {
			_ = raw.Close()
			t.Fatalf("write the proxy header: %v", err)
		}
	}
	conn := tls.Client(raw, conf)
	if err := conn.HandshakeContext(t.Context()); err != nil {
		_ = raw.Close()
		t.Fatalf("tls handshake: %v", err)
	}
	return &esme{conn: conn}, conn
}

func TestAnESMEBindsOverTLSWithoutPresentingACertificate(t *testing.T) {
	pool := pgtest.Pool(t)
	registry := startRegistry(t, redistest.Client(t))
	serverConf, caFile := smppTLSServer(t)

	sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
	addr := startListener(t, pool, registry, func(o *smppserver.Options) { o.TLSConfig = serverConf })

	e, conn := dialTLSESME(t, addr, "", esmeTLSConfig(t, caFile))
	defer e.close()

	if got := conn.ConnectionState().Version; got != tls.VersionTLS12 {
		t.Errorf("negotiated TLS version = %#x, want 1.2 — the public floor did not reach the handshake", got)
	}
	if got := e.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("bind status = %#x, want ESME_ROK", got)
	}
}

func TestAPlaintextESMEIsRefusedOnATLSPort(t *testing.T) {
	pool := pgtest.Pool(t)
	registry := startRegistry(t, redistest.Client(t))
	serverConf, _ := smppTLSServer(t)

	sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
	addr := startListener(t, pool, registry, func(o *smppserver.Options) { o.TLSConfig = serverConf })

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	body := &smpp.BindTransceiver{BindFields: smpp.BindFields{
		SystemID: sid, Password: pw, InterfaceVersion: smpp.InterfaceVersion34,
	}}
	if err := smpp.WritePDU(conn, smpp.PDU{Sequence: 1, Body: body}); err != nil {
		return
	}
	if _, err := smpp.ReadPDU(conn); err == nil {
		t.Fatal("a plaintext bind was answered on a TLS listener")
	}
}

func TestTheProxyHeaderIsReadBeforeTheTLSHandshake(t *testing.T) {
	pool := pgtest.Pool(t)
	registry := startRegistry(t, redistest.Client(t))
	serverConf, caFile := smppTLSServer(t)

	sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
	addr := startListener(t, pool, registry, func(o *smppserver.Options) {
		o.TLSConfig = serverConf
		o.TrustedProxyCIDRs = []string{"127.0.0.0/8"}
	})

	e, _ := dialTLSESME(t, addr, "PROXY TCP4 203.0.113.7 198.51.100.1 51000 2775\r\n", esmeTLSConfig(t, caFile))
	defer e.close()

	if got := e.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("bind status = %#x, want ESME_ROK", got)
	}
}

func TestTheSMPPPortStaysPlaintextWhenNoTLSConfigIsWired(t *testing.T) {
	pool := pgtest.Pool(t)
	registry := startRegistry(t, redistest.Client(t))

	sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
	addr := startListener(t, pool, registry)

	e := dialESME(t, addr)
	defer e.close()
	if got := e.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("bind status = %#x, want ESME_ROK", got)
	}
}
