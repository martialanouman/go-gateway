package modlrrouter

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/configsecrets/pb"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// openTimeout bounds the key-service hop. Without it, Open inherits the delivery consumer's long-lived
// context: a content-key-svc that is reachable but wedged blocks the serial goroutine and with it the whole
// partition's return traffic — the head-of-line failure the webhook HTTP client is given a timeout for ten
// lines away in the same wiring.
const openTimeout = 5 * time.Second

// GRPCSecretOpener opens a sealed webhook signing secret through content-key-svc.
type GRPCSecretOpener struct {
	client pb.ConfigSecretsClient
}

// NewGRPCSecretOpener returns the opener the webhook sender signs with.
func NewGRPCSecretOpener(client pb.ConfigSecretsClient) *GRPCSecretOpener {
	return &GRPCSecretOpener{client: client}
}

// Open returns the clear signing key, marking only reachability failures as ErrServiceUnavailable.
func (o *GRPCSecretOpener) Open(ctx context.Context, sealed cp.SealedSecret) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()

	resp, err := o.client.Open(ctx, &pb.OpenRequest{Sealed: sealed.Sealed})
	if err != nil {
		if deterministicOpenFailure(status.Code(err)) {
			return nil, fmt.Errorf("open config secret: %w", err)
		}
		return nil, fmt.Errorf("open config secret: %w: %w", errs.ErrServiceUnavailable, err)
	}
	if len(resp.GetPlaintext()) == 0 {
		return nil, fmt.Errorf("open config secret: empty plaintext: %w", errs.ErrInternal)
	}
	return resp.GetPlaintext(), nil
}

// deterministicOpenFailure names the codes that belong to the CIPHERTEXT, so the sender dead-letters them.
// It is a deny-list because the enum is open-ended and the two mistakes do not cost the same: treating a
// permanent fault as transient stalls a partition, which an operator sees, while treating a transient one
// as permanent destroys a backlog, which nobody sees. PermissionDenied in particular is a property of the
// deployment — a rollout that updates one image before the other — and an operator fixes it without
// touching a single row.
func deterministicOpenFailure(code codes.Code) bool {
	switch code {
	case codes.InvalidArgument: // sealed for another domain: these bytes are not a config secret
		return true
	case codes.Internal: // the KMS refused to unwrap: tampered, truncated, or sealed under a lost key
		return true
	default:
		return false
	}
}
