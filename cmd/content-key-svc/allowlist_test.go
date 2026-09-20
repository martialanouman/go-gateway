package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/config"
	configsecretspb "github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	contentkeypb "github.com/martialanouman/go-gateway/internal/contentkeys/pb"
	"github.com/martialanouman/go-gateway/internal/grpctls"
	"github.com/martialanouman/go-gateway/internal/testutil/grpctest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// The wiring, not the mechanism: grpctls proves an allowlist refuses a stranger and proves nothing
// about whether this service passes one. Sole holder of the KMS (ADR-0011), it returns a customer's
// plaintext data key — a tunnel without authorisation lets any pod of the cluster ask for any key.
func TestTheWiredServerAdmitsOnlyTheCallersItNames(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.Issue(t, serviceName, serviceName)

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.TLS = config.TLS{
		Enabled:        true,
		CertFile:       certFile,
		KeyFile:        keyFile,
		ClientCAFile:   ca.CAFile,
		AllowedClients: []string{"router-svc", "admin-api-svc"},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newContentKeyApp(ctx, cfg, discardLogger())
	if err != nil {
		t.Fatalf("newContentKeyApp: %v", err)
	}
	defer app.close()

	addr := grpctest.Serve(t, app.grpc)

	// Named by the configuration: the Unimplemented is the probe's own method, not a refusal.
	if code, err := grpctest.Probe(t, dialAs(t, ca, addr, "router-svc")); code != codes.Unimplemented {
		t.Fatalf("a named caller answered %s (%v), want Unimplemented", code, err)
	}

	// Same authority, not named. Without the allowlist reaching the server this call succeeds too.
	//
	// The named caller above is the control, and the message is not asserted: under TLS 1.3 the client
	// finishes its handshake before the server validates its certificate, so it reads either the alert
	// or a broken pipe depending on which wins.
	if code, err := grpctest.Probe(t, dialAs(t, ca, addr, "connector-pool-svc")); code == codes.Unimplemented {
		t.Fatalf("a caller the configuration does not name reached the key service (%s, %v)", code, err)
	}
}

// dialAs builds a client holding a certificate issued to name. The identity verified is pinned because
// the address is a loopback port, which no certificate of ours carries.
func dialAs(t *testing.T, ca *tlstest.CA, addr, name string) *grpc.ClientConn {
	t.Helper()
	certFile, keyFile := ca.Issue(t, name, name)
	dial, err := grpctls.Dialer(config.TLS{
		Enabled:      true,
		CertFile:     certFile,
		KeyFile:      keyFile,
		ClientCAFile: ca.CAFile,
	}, discardLogger(), serviceName)
	if err != nil {
		t.Fatalf("Dialer: %v", err)
	}
	conn, err := dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// The TLS allowlist admits a BINARY, not a method: it is checked during the handshake (tlsconf.ServerConfig)
// and nothing below it looks at what is being called. Registering ConfigSecrets on this listener therefore
// handed router-svc — a hot-path service that also holds POSTGRES_URL — the ability to read
// smsc_connectors.password_sealed and open every outbound bind password. That capability existed nowhere
// before step-295, since the column held argon2id hashes.
//
// router-svc needs ContentKeys and only ContentKeys. This asserts the two answers differ for it.
func TestConfigSecretsIsRefusedToCallersThatOnlyNeedContentKeys(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.Issue(t, serviceName, serviceName)

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.TLS = config.TLS{
		Enabled:        true,
		CertFile:       certFile,
		KeyFile:        keyFile,
		ClientCAFile:   ca.CAFile,
		AllowedClients: []string{"router-svc", "admin-api-svc"},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newContentKeyApp(ctx, cfg, discardLogger())
	if err != nil {
		t.Fatalf("newContentKeyApp: %v", err)
	}
	defer app.close()

	addr := grpctest.Serve(t, app.grpc)

	// The data plane keeps what it came for: a real ContentKeys call, refused for its own reasons (no such
	// customer) and not for authorisation.
	routerConn := dialAs(t, ca, addr, "router-svc")
	_, err = contentkeypb.NewContentKeysClient(routerConn).GetOrCreateContentKey(ctx,
		&contentkeypb.GetOrCreateContentKeyRequest{CustomerId: uuid.NewString()})
	if code := status.Code(err); code == codes.PermissionDenied {
		t.Errorf("router-svc was refused ContentKeys, which it legitimately needs: %v", err)
	}

	// The secrets it has no business with.
	_, err = configsecretspb.NewConfigSecretsClient(routerConn).Seal(ctx,
		&configsecretspb.SealRequest{Plaintext: []byte("x")})
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Errorf("Seal from router-svc = %s (%v), want PermissionDenied", code, err)
	}
	_, err = configsecretspb.NewConfigSecretsClient(routerConn).Open(ctx,
		&configsecretspb.OpenRequest{Sealed: []byte("x")})
	if code := status.Code(err); code != codes.PermissionDenied {
		t.Errorf("Open from router-svc = %s (%v), want PermissionDenied", code, err)
	}

	// admin-api-svc writes these secrets, so it must still be served.
	adminConn := dialAs(t, ca, addr, "admin-api-svc")
	if _, err := configsecretspb.NewConfigSecretsClient(adminConn).Seal(ctx,
		&configsecretspb.SealRequest{Plaintext: []byte("x")}); err != nil {
		t.Errorf("Seal from admin-api-svc was refused: %v", err)
	}
}

// A certificate normally carries several DNS SANs — cert-manager fills them from a Certificate's spec,
// and the service name is rarely the first. This gate and the handshake allowlist must read the same
// identity from the same certificate: reading only DNSNames[0] made them disagree, admitting a caller at
// the handshake and then refusing it here, or the reverse.
func TestAuthorisationReadsEverySANLikeTheHandshakeAllowlistDoes(t *testing.T) {
	ca := tlstest.NewCA(t)
	certFile, keyFile := ca.Issue(t, serviceName, serviceName)

	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.TLS = config.TLS{
		Enabled: true, CertFile: certFile, KeyFile: keyFile, ClientCAFile: ca.CAFile,
		AllowedClients: []string{"admin-api-svc"},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newContentKeyApp(ctx, cfg, discardLogger())
	if err != nil {
		t.Fatalf("newContentKeyApp: %v", err)
	}
	defer app.close()
	addr := grpctest.Serve(t, app.grpc)

	// The pod's own DNS name first, the service identity after — the shape a Certificate produces.
	clientCert, clientKey := ca.Issue(t, "admin-api-svc", "admin-api-svc-7d9f.ns.svc", "admin-api-svc")
	dial, err := grpctls.Dialer(config.TLS{
		Enabled: true, CertFile: clientCert, KeyFile: clientKey, ClientCAFile: ca.CAFile,
	}, discardLogger(), serviceName)
	if err != nil {
		t.Fatalf("Dialer: %v", err)
	}
	conn, err := dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := configsecretspb.NewConfigSecretsClient(conn).Seal(ctx,
		&configsecretspb.SealRequest{Plaintext: []byte("x")}); err != nil {
		t.Errorf("a caller the handshake admitted was refused here: %v", err)
	}
}
