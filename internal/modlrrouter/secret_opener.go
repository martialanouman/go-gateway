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

type GRPCSecretOpener struct {
	client pb.ConfigSecretsClient
}

func NewGRPCSecretOpener(client pb.ConfigSecretsClient) *GRPCSecretOpener {
	return &GRPCSecretOpener{client: client}
}

func (o *GRPCSecretOpener) Open(ctx context.Context, sealed cp.SealedSecret) ([]byte, error) {
	resp, err := o.client.Open(ctx, &pb.OpenRequest{Sealed: sealed.Sealed})
	if err != nil {
		// Only reachability can change on another attempt. A refused authorisation, a wrong domain tag or a
		// ciphertext the KMS will not unwrap answer identically forever, and the sender dead-letters those.
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
			return nil, fmt.Errorf("open config secret: %w: %w", errs.ErrServiceUnavailable, err)
		default:
			return nil, fmt.Errorf("open config secret: %w", err)
		}
	}
	return resp.GetPlaintext(), nil
}
