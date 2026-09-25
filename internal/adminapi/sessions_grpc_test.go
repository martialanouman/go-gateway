package adminapi

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
)

type downRegistry struct {
	registrypb.SessionRegistryClient
	code codes.Code
}

func (d downRegistry) ListSessions(context.Context, *registrypb.ListSessionsRequest, ...grpc.CallOption) (*registrypb.ListSessionsResponse, error) {
	return nil, status.Error(d.code, "down")
}

func (d downRegistry) Disconnect(context.Context, *registrypb.DisconnectRequest, ...grpc.CallOption) (*registrypb.DisconnectResponse, error) {
	return nil, status.Error(d.code, "down")
}

// TestARegistryOutageIsRetryable: a redeploying session-manager must read as a 503 the dashboard retries,
// not a 500 that reads as a bug.
func TestARegistryOutageIsRetryable(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded} {
		g := NewGRPCSessions(downRegistry{code: code})
		_, _, listErr := g.ListSessions(context.Background(), "", 10)
		discErr := g.DisconnectSession(context.Background(), "b", OperatorDisconnectReason)
		for _, err := range []error{listErr, discErr} {
			if !errors.Is(err, errs.ErrServiceUnavailable) {
				t.Errorf("%v: err = %v, want ErrServiceUnavailable", code, err)
			}
		}
	}
}
