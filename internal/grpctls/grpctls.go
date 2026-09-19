// Package grpctls turns a pod's TLS configuration into the two gRPC options a binary needs: one for
// what it serves, one for what it dials. It is the only place the on/off switch is read.
//
// It is not in internal/platform/tlsconf because that package knows nothing beyond the standard
// library, and the HTTP and SMPP sides must not pull gRPC in to get a tls.Config.
package grpctls

import (
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
)

// ServerOption is the transport credentials of a gRPC surface: mutual TLS against the configured
// authority, restricted to cfg.AllowedClients when it names any.
//
// The ALPN list is left empty, and gRPC is the one transport where that is right: credentials.NewTLS
// appends "h2" to whatever GetConfigForClient returns. net/http does not.
func ServerOption(cfg config.TLS, logger *slog.Logger) (grpc.ServerOption, error) {
	if !cfg.Enabled {
		return grpc.EmptyServerOption{}, nil
	}
	conf, err := files(cfg, logger).ServerConfig(tlsconf.ServerOptions{AllowedClients: cfg.AllowedClients})
	if err != nil {
		return nil, err
	}
	return grpc.Creds(credentials.NewTLS(conf)), nil
}

// DialOption is the transport credentials of a gRPC client. The name verified is the authority of the
// address dialled: with ServerName empty, grpc-go's ClientHandshake fills it from that authority.
func DialOption(cfg config.TLS, logger *slog.Logger) (grpc.DialOption, error) {
	return DialOptionTo(cfg, logger, "")
}

// DialOptionTo is DialOption for a peer whose verified identity is not the address dialled — the return
// path, which reaches an individual pod while the certificate is issued per Deployment. An empty
// serverName is DialOption.
//
// A pinned option must NOT be shared between two dial targets: grpc-go reads the name off the
// credentials to set a connection's authority, so both would be verified — and addressed — as the one
// name. Sharing an unpinned option is safe, and admin-api-svc does it.
func DialOptionTo(cfg config.TLS, logger *slog.Logger, serverName string) (grpc.DialOption, error) {
	if !cfg.Enabled {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	conf, err := files(cfg, logger).ClientConfig()
	if err != nil {
		return nil, err
	}
	conf.ServerName = serverName
	return grpc.WithTransportCredentials(credentials.NewTLS(conf)), nil
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
