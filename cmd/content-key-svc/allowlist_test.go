package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/grpctls"
	"github.com/martialanouman/go-gateway/internal/testutil/grpctest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// TestTheWiredServerAdmitsOnlyTheCallersItNames is about the WIRING, not the mechanism: grpctls proves
// an allowlist refuses a stranger, and proves nothing about whether this service passes one.
//
// It is the link that matters most in the repository. This service is the sole holder of the KMS
// (ADR-0011), and GetContentEncryptionKey returns a customer's plaintext data key. A tunnel without
// authorisation lets any pod of the cluster ask for any key: holding a certificate of our authority
// proves a peer is one of our pods, never WHICH one.
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

	// router-svc is one of the two callers the configuration names: it reaches the server, and the
	// Unimplemented is the probe's own method, not a refusal.
	if code, err := grpctest.Probe(t, dialAs(t, ca, addr, "router-svc")); code != codes.Unimplemented {
		t.Fatalf("a named caller answered %s (%v), want Unimplemented", code, err)
	}

	// connector-pool-svc holds a certificate of the same authority and is not named. Without the
	// allowlist reaching the server, this call succeeds exactly like the one above.
	code, err := grpctest.Probe(t, dialAs(t, ca, addr, "connector-pool-svc"))
	if code == codes.Unimplemented {
		t.Fatal("a caller the configuration does not name reached the key service")
	}
	if err == nil || !strings.Contains(err.Error(), "bad certificate") {
		t.Fatalf("refusal not attributed to the certificate: %v", err)
	}
}

// dialAs builds a client holding a certificate issued to name, verifying the key service under its own
// identity — the address is a loopback port, which no certificate of ours carries.
func dialAs(t *testing.T, ca *tlstest.CA, addr, name string) *grpc.ClientConn {
	t.Helper()
	certFile, keyFile := ca.Issue(t, name, name)
	creds, err := grpctls.DialOptionTo(config.TLS{
		Enabled:      true,
		CertFile:     certFile,
		KeyFile:      keyFile,
		ClientCAFile: ca.CAFile,
	}, discardLogger(), serviceName)
	if err != nil {
		t.Fatalf("DialOptionTo: %v", err)
	}
	conn, err := grpc.NewClient(addr, creds)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
