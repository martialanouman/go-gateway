// Package grpctls builds every gRPC server and every gRPC client of this repository, so that the pod's
// TLS identity cannot be forgotten at a call site. Production code never calls grpc.NewServer or
// grpc.NewClient itself, and a guard in this package's tests holds that — a rule about which function is
// called, which no reassignment, import alias or deprecated spelling walks around.
//
// It is not in internal/platform/tlsconf because that package knows nothing beyond the standard
// library, and the HTTP and SMPP sides must not pull gRPC in to get a tls.Config.
package grpctls

import (
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
)

// Dial opens one connection. It is what Dialer returns, named so the callers that pass it around do not
// each spell the signature.
type Dial func(addr string) (*grpc.ClientConn, error)

// NewServer builds a gRPC server carrying the pod's identity: mutual TLS against the configured
// authority, restricted to cfg.AllowedClients when it names any.
//
// The ALPN list is left empty, and gRPC is the one transport where that is right: credentials.NewTLS
// appends "h2" to whatever GetConfigForClient returns. net/http does not.
// opts are appended to whatever this function builds, for a service that needs more than transport
// security — an interceptor, say. The TLS allowlist admits a BINARY, decided during the handshake; a
// service whose port serves more than one audience needs its own say on top, and content-key-svc does.
func NewServer(cfg config.TLS, logger *slog.Logger, opts ...grpc.ServerOption) (*grpc.Server, error) {
	if !cfg.Enabled {
		return grpc.NewServer(opts...), nil
	}
	conf, err := files(cfg, logger).ServerConfig(tlsconf.ServerOptions{AllowedClients: cfg.AllowedClients})
	if err != nil {
		return nil, err
	}
	return grpc.NewServer(append(opts, grpc.Creds(credentials.NewTLS(conf)))...), nil
}

// NewClient dials addr with the pod's identity, verifying the peer under the name addr carries. It opens
// no connection: gRPC connects on the first call.
func NewClient(cfg config.TLS, logger *slog.Logger, addr string) (*grpc.ClientConn, error) {
	dial, err := Dialer(cfg, logger, "")
	if err != nil {
		return nil, err
	}
	return dial(addr)
}

// Dialer returns a dial function, for a caller that opens one connection per peer — PodClients, which
// holds one per smpp-server pod.
//
// serverName is the identity to verify when it is NOT the address dialled: the return path reaches an
// individual pod while the certificate is issued per Deployment, so the one thing that certificate can
// attest is that the peer belongs to it. Empty verifies the address, which is what every other caller
// wants. A dialer built WITH a serverName addresses EVERY target it dials as that name: grpc-go reads
// it off the credentials to set each connection's authority. That is correct only for peers sharing one
// identity — the replicas of a Deployment, which share a certificate — and it is how the return path
// uses it, dialling one address per smpp-server pod while verifying the one name they all present
// (step-302). Pointing such a dialer at peers with DIFFERENT identities would address them all as the
// first, and verify the wrong one.
func Dialer(cfg config.TLS, logger *slog.Logger, serverName string) (Dial, error) {
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	if cfg.Enabled {
		conf, err := files(cfg, logger).ClientConfig()
		if err != nil {
			return nil, err
		}
		conf.ServerName = serverName
		creds = grpc.WithTransportCredentials(credentials.NewTLS(conf))
	}
	return func(addr string) (*grpc.ClientConn, error) {
		conn, err := grpc.NewClient(addr, creds)
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		return conn, nil
	}, nil
}

// files projects the configuration onto the three paths tlsconf reads.
func files(cfg config.TLS, logger *slog.Logger) tlsconf.Files {
	return tlsconf.Files{
		Cert:     cfg.CertFile,
		Key:      cfg.KeyFile,
		ClientCA: cfg.ClientCAFile,
		Logger:   logger,
	}
}
