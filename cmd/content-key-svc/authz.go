package main

import (
	"context"
	"crypto/x509"
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
	name, ok := peerServiceName(ctx)
	if !ok {
		return nil
	}
	if !configSecretsCallers[name] {
		return status.Errorf(codes.PermissionDenied, "%s is not allowed to call config secrets", name)
	}
	return nil
}

// peerServiceName returns the DNS SAN the caller's certificate carries, matching what tlsconf's allowlist
// matches on. ok is false when the connection carries no verified peer certificate — no TLS, or no client
// certificate — in which case there is no identity to decide on.
func peerServiceName(ctx context.Context) (string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", false
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", false
	}
	for _, chain := range tlsInfo.State.VerifiedChains {
		if len(chain) > 0 {
			return firstDNSName(chain[0]), chain[0] != nil
		}
	}
	return "", false
}

func firstDNSName(cert *x509.Certificate) string {
	if cert == nil || len(cert.DNSNames) == 0 {
		return ""
	}
	return cert.DNSNames[0]
}

// configSecretsUnaryInterceptor applies authorizeConfigSecrets before any handler runs.
func configSecretsUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := authorizeConfigSecrets(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}
