package modlrrouter

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// GRPCSecretOpener opens a webhook's sealed signing secret through content-key-svc, which holds the master
// key and nothing else does (ADR-0011, ADR-0016). It is this repository's FIRST production caller of
// ConfigSecrets.Open: the two secrets step-295 sealed are still never opened anywhere.
type GRPCSecretOpener struct {
	client pb.ConfigSecretsClient
}

// NewGRPCSecretOpener returns the opener the webhook sender signs with.
func NewGRPCSecretOpener(client pb.ConfigSecretsClient) *GRPCSecretOpener {
	return &GRPCSecretOpener{client: client}
}

// Open returns the clear signing key. Its error classification is half of a decision the webhook sender
// completes: the sender redelivers the event when the error carries ErrServiceUnavailable and dead-letters
// it otherwise, so what belongs in the transient set is exactly what ANOTHER attempt could change.
//
// Only reachability qualifies. A refused authorisation, a wrong domain tag or a ciphertext the KMS will not
// unwrap are properties of this row and this deployment: they answer the same way on every redelivery, and
// treating them as transient would hold the partition — with every other account's MO and DLR behind it —
// on one unopenable webhook.
//
// The key reference is deliberately not sent: the KMS unwraps with the master key it holds. It is stored
// beside the ciphertext so a future rotation knows which key sealed which row, not to be replayed here.
func (o *GRPCSecretOpener) Open(ctx context.Context, sealed cp.SealedSecret) ([]byte, error) {
	resp, err := o.client.Open(ctx, &pb.OpenRequest{Sealed: sealed.Sealed})
	if err != nil {
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
			return nil, fmt.Errorf("open config secret: %w: %w", errs.ErrServiceUnavailable, err)
		default:
			return nil, fmt.Errorf("open config secret: %w", err)
		}
	}
	return resp.GetPlaintext(), nil
}
