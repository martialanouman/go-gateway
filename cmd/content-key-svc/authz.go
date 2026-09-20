package main

import (
	"context"
	"maps"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// configSecretsCallers are the services allowed to call ConfigSecrets. It is NOT configuration: which
// service may seal a control-plane secret is a property of this service, and a deployment that could widen
// it by an environment variable would widen it by accident.
//
// connector-pool-svc joins this list the day it reads its bind password from the control plane rather than
// from CONNECTOR_PASSWORD — on Open alone. See debts/ancre-de-confiance-par-connecteur.md.
var configSecretsCallers = map[string]bool{"admin-api-svc": true}

// authorizeConfigSecrets refuses ConfigSecrets to callers that are admitted to this port for ContentKeys.
//
// The TLS allowlist cannot do it: it is checked during the handshake, so it admits a BINARY and never sees
// which method follows. router-svc legitimately needs ContentKeys, and registering ConfigSecrets beside it
// otherwise hands that same hot-path pod — which also holds POSTGRES_URL — the means to read
// smsc_connectors.password_sealed and open every outbound bind password. No caller had that before
// step-295, when the column still held argon2id hashes.
//
// With TLS disabled there is no peer identity to read, and this returns nothing: the same posture as the
// allowlist itself, which only applies when TLS is on. A deployment without mTLS has no authorisation
// story at all, which is what the production config validator is for.
func authorizeConfigSecrets(ctx context.Context, fullMethod string) error {
	if !strings.HasPrefix(fullMethod, "/configsecrets.") {
		return nil
	}
	names, identified := peerDNSNames(ctx)
	if !identified {
		return nil
	}
	for _, name := range names {
		if configSecretsCallers[name] {
			return nil
		}
	}
	// Both halves are named, as tlsconf's own allowlist does: neither the identity presented nor the one
	// expected is a secret, and a refusal that said neither would be undebuggable.
	return status.Errorf(codes.PermissionDenied, "client identity %v is not allowed to call config secrets %v",
		names, slices.Sorted(maps.Keys(configSecretsCallers)))
}

// peerDNSNames returns the DNS SANs of the caller's verified certificate — ALL of them, matching what
// tlsconf.allowlist matches on. Reading only the first would make this gate and the handshake allowlist
// disagree about the same certificate, and a multi-SAN certificate is the normal shape under cert-manager.
//
// identified is false when the connection carries no verified peer certificate — TLS disabled, or no
// client certificate. There is then no identity to decide on, which is the same posture as the allowlist
// itself: it only applies when TLS is on, and a deployment without mTLS has no authorisation story that
// this interceptor could rescue.
func peerDNSNames(ctx context.Context) ([]string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, false
	}
	for _, chain := range tlsInfo.State.VerifiedChains {
		if len(chain) > 0 && chain[0] != nil {
			return chain[0].DNSNames, true
		}
	}
	return nil, false
}

// configSecretsUnaryInterceptor applies authorizeConfigSecrets before any handler runs.
func configSecretsUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := authorizeConfigSecrets(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// configSecretsStreamInterceptor is the same gate for streaming RPCs. This service has none today, and
// that is exactly why it is here: a stream added to ConfigSecrets later would otherwise be served to
// everyone the TLS allowlist admits, and nothing would say so.
func configSecretsStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := authorizeConfigSecrets(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}
