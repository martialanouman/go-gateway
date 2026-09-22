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
		// Only a verdict on the CIPHERTEXT is permanent: a wrong domain tag, or a blob the KMS cannot unwrap.
		// Everything else is a deny-list mistake waiting to happen, and the two errors do not cost the same —
		// treating a permanent fault as transient stalls a partition, which an operator sees, while treating a
		// transient one as permanent destroys a backlog, which nobody sees. PermissionDenied in particular is
		// a property of the deployment, and Internal is what grpc-go reports for a transport fault.
		switch status.Code(err) {
		case codes.InvalidArgument, codes.DataLoss:
			return nil, fmt.Errorf("open config secret: %w", err)
		}
		return nil, fmt.Errorf("open config secret: %w: %w", errs.ErrServiceUnavailable, err)
	}
	return resp.GetPlaintext(), nil
}
