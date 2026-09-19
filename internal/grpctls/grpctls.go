// Package grpctls turns a pod's TLS configuration into the two gRPC options every binary of this
// repository needs: one for the surfaces it serves, one for the peers it dials. It is the only place
// the on/off switch is read, so a service cannot end up serving in TLS while dialling in plaintext.
//
// It sits here rather than in internal/platform/tlsconf because that package deliberately knows nothing
// beyond the standard library: 300c serves HTTP from it and 300d wraps SMPP in it, and neither should
// pull gRPC in to get a tls.Config.
package grpctls

import (
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
)

// ServerOption is the transport credentials of a gRPC surface: mutual TLS, verified against the
// configured authority, and — when the configuration names callers — restricted to them.
//
// cfg.AllowedClients travels with the identity because it belongs to the pod, not to the surface: a
// binary serving something only some callers may have names them in its own environment.
//
// The ALPN list is left empty on purpose, and this is the one transport where that is right:
// credentials.NewTLS wraps GetConfigForClient and appends "h2" to whatever the callback returns, so
// gRPC supplies the protocol this package would otherwise have to know. net/http does not, which is why
// the HTTP side must pass its own complete list.
func ServerOption(cfg config.TLS, logger *slog.Logger) (grpc.ServerOption, error) {
	if !cfg.Enabled {
		// EmptyServerOption rather than a nil option: grpc.NewServer dereferences what it is given, and a
		// caller writing one line cannot branch on a nil it never sees.
		return grpc.EmptyServerOption{}, nil
	}
	conf, err := files(cfg, logger).ServerConfig(tlsconf.ServerOptions{AllowedClients: cfg.AllowedClients})
	if err != nil {
		return nil, err
	}
	return grpc.Creds(credentials.NewTLS(conf)), nil
}

// DialOption is the transport credentials of a gRPC client, verifying the peer against the configured
// authority and presenting this pod's own certificate.
//
// The name verified is the authority of the address dialled: grpc-go fills ServerName from it when the
// config leaves it empty, so a caller reaching a Service by its name verifies that name and has nothing
// to declare. DialOptionTo covers the one case where the two differ.
func DialOption(cfg config.TLS, logger *slog.Logger) (grpc.DialOption, error) {
	return DialOptionTo(cfg, logger, "")
}

// DialOptionTo is DialOption for a peer whose verified identity is NOT the address dialled. An empty
// serverName is DialOption.
//
// It exists for the return path, which addresses an individual smpp-server pod through a headless
// service while the certificate is issued per Deployment: the only identity that certificate can attest
// is "a pod of that Deployment", and pinning the name says so out loud instead of demanding a per-pod
// SAN no authority here issues.
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

// files projects the configuration onto the three paths tlsconf reads. The projection is here and not in
// eight wiring files so that a fourth path, the day one appears, is added once.
func files(cfg config.TLS, logger *slog.Logger) tlsconf.Files {
	return tlsconf.Files{
		Cert:     cfg.CertFile,
		Key:      cfg.KeyFile,
		ClientCA: cfg.ClientCAFile,
		Logger:   logger,
	}
}
