package adminapi

import (
	"context"

	"github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// GRPCSecretSealer adapts the ConfigSecrets gRPC client to the SecretSealer the connector and billing
// handlers use. Like content-key rotation, the Admin API DELEGATES the crypto rather than holding a KMS:
// the master key lives in content-key-svc and nowhere else (ADR-0011, ADR-0016).
type GRPCSecretSealer struct {
	client pb.ConfigSecretsClient
}

// NewGRPCSecretSealer returns a SecretSealer backed by the ConfigSecrets client.
func NewGRPCSecretSealer(client pb.ConfigSecretsClient) *GRPCSecretSealer {
	return &GRPCSecretSealer{client: client}
}

// Seal delegates to content-key-svc. The error is returned as-is: the two callers wrap it into an opaque
// internal failure, so no detail of the secret — nor of why the key service refused — reaches a client.
func (s *GRPCSecretSealer) Seal(ctx context.Context, plaintext []byte) (cp.SealedSecret, error) {
	resp, err := s.client.Seal(ctx, &pb.SealRequest{Plaintext: plaintext})
	if err != nil {
		return cp.SealedSecret{}, err
	}
	return cp.SealedSecret{Sealed: resp.GetSealed(), KMSKeyRef: resp.GetKmsKeyRef()}, nil
}
