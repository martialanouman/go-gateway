package main

import (
	"context"
	"slices"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// configSecretsCallers and contentKeysCallers are NOT configuration: an environment variable would widen
// them by accident. Both fail closed — a caller or method not named here is refused.
//
// Seal would be an escalation for mo-dlr-router-svc, not a convenience: it holds POSTGRES_URL with write
// access, so it could seal a password of its choosing, write it into smsc_connectors.password_sealed and
// take over an outbound operator bind.
var configSecretsCallers = map[string][]string{
	"admin-api-svc":     {"/configsecrets.ConfigSecrets/Seal"},
	"mo-dlr-router-svc": {"/configsecrets.ConfigSecrets/Open"},
}

// contentKeysCallers gates the OTHER service on this port. The mTLS allowlist used to be its only
// authorisation, so admitting a caller for ConfigSecrets alone also handed it GetContentEncryptionKey — a
// customer's plaintext data key — and DestroyContentKeys, an irreversible crypto-shred. mo-dlr-router-svc
// needs no ContentKeys method at all.
//
// By service and not by method: narrowing the two callers that legitimately use ContentKeys is a separate
// decision, and getting it wrong breaks the data plane's ability to encrypt a body.
var contentKeysCallers = map[string]bool{"router-svc": true, "admin-api-svc": true}

// authorizeKeyService decides both services on this port: ConfigSecrets by method, ContentKeys by caller.
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
func authorizeKeyService(ctx context.Context, fullMethod string) error {
	configSecrets := strings.HasPrefix(fullMethod, "/configsecrets.")
	contentKeys := strings.HasPrefix(fullMethod, "/contentkeys.")
	if !configSecrets && !contentKeys {
		return nil
	}
	names, identified := peerDNSNames(ctx)
	if !identified {
		return nil
	}
	for _, name := range names {
		if configSecrets && slices.Contains(configSecretsCallers[name], fullMethod) {
			return nil
		}
		if contentKeys && contentKeysCallers[name] {
			return nil
		}
	}
	// Only the identity presented, which the caller already knows. tlsconf's allowlist names both halves,
	// but its message never reaches anyone — TLS answers "bad certificate" and cuts. This one travels the
	// wire, so the list of who IS allowed stays on this side.
	return status.Errorf(codes.PermissionDenied, "client identity %v is not allowed to call %s", names, fullMethod)
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

// keyServiceUnaryInterceptor applies authorizeKeyService before any handler runs.
func keyServiceUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := authorizeKeyService(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// keyServiceStreamInterceptor is the same gate for streaming RPCs. This service has none today, and
// that is exactly why it is here: a stream added to ConfigSecrets later would otherwise be served to
// everyone the TLS allowlist admits, and nothing would say so.
func keyServiceStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := authorizeKeyService(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}
