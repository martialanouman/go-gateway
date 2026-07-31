package content

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
)

// GRPCDataKeyFetcher fetches a customer's active DEK from billing-svc (the sole KMS holder) via the guarded
// GetContentEncryptionKey RPC. It is the remote source behind DataKeyCache. The returned DEK is sensitive and
// must never be logged.
type GRPCDataKeyFetcher struct {
	client pb.ContentKeysClient
}

// NewGRPCDataKeyFetcher returns a DataKeyFetcher backed by the ContentKeys client.
func NewGRPCDataKeyFetcher(client pb.ContentKeysClient) *GRPCDataKeyFetcher {
	return &GRPCDataKeyFetcher{client: client}
}

// Fetch calls billing-svc for the customer's active key id and plaintext DEK.
func (f *GRPCDataKeyFetcher) Fetch(ctx context.Context, customerID uuid.UUID) (DataKey, error) {
	resp, err := f.client.GetContentEncryptionKey(ctx, &pb.GetContentEncryptionKeyRequest{CustomerId: customerID.String()})
	if err != nil {
		return DataKey{}, fmt.Errorf("fetch content encryption key: %w", err)
	}
	keyID, err := uuid.Parse(resp.GetKeyId())
	if err != nil {
		return DataKey{}, fmt.Errorf("fetch content encryption key: bad key id: %w", err)
	}
	if len(resp.GetDek()) != dekSize {
		return DataKey{}, fmt.Errorf("fetch content encryption key: dek is %d bytes, want %d", len(resp.GetDek()), dekSize)
	}
	// Accepted limit: the plaintext DEK also lives in the decoded protobuf message (and gRPC's decode buffer),
	// which we cannot zeroize — it lingers until GC. The cache's copy/zeroize hygiene bounds the *cached*
	// copy's lifetime, not this transient one; a real KMS SDK with secure buffers would be the full fix.
	return DataKey{KeyID: keyID, DEK: resp.GetDek()}, nil
}
