package main

import (
	"context"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// keyServiceCallers is the complete authorisation of this port, by caller and by FULL method. It is NOT
// configuration: an environment variable would widen it by accident. Everything absent is refused —
// including an RPC nobody has claimed yet, and including a third service registered here later.
//
// The mTLS allowlist cannot do this: it is checked at the handshake, so it admits a BINARY and never sees
// the method. Before this gate, admitting a caller for one service handed it the other's — which is how
// step-295b's own first cut gave the return path GetContentEncryptionKey, a customer's plaintext data key,
// and DestroyContentKeys, an irreversible crypto-shred.
//
// Each list is what its caller's code actually calls, and nothing more. GetOrCreateContentKey appears
// nowhere because no production caller invokes it.
var keyServiceCallers = map[string][]string{
	"router-svc": {
		"/contentkeys.ContentKeys/GetContentEncryptionKey",
	},
	"admin-api-svc": {
		"/configsecrets.ConfigSecrets/Seal",
		"/contentkeys.ContentKeys/RotateContentKey",
		"/contentkeys.ContentKeys/GetContentKey",
		"/contentkeys.ContentKeys/DestroyContentKeys",
	},
	"mo-dlr-router-svc": {
		"/configsecrets.ConfigSecrets/Open",
	},
}

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
	names, identified := peerDNSNames(ctx)
	if !identified {
		return nil
	}
	for _, name := range names {
		if slices.Contains(keyServiceCallers[name], fullMethod) {
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
