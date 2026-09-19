// Package grpctest serves a gRPC server on a real socket and probes it, for tests about the transport
// rather than the services above it.
package grpctest

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// probeMethod is implemented by nothing, so a completed handshake answers Unimplemented and a refused
// one Unavailable.
const probeMethod = "/grpctest.Probe/Ping"

// Serve starts srv on a loopback port and returns its address, stopping it when the test ends.
func Serve(t *testing.T, srv *grpc.Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// Probe runs one RPC over conn. It returns the whole error and not just the code because a reset
// connection and a refused certificate both answer Unavailable, and only the message tells them apart.
func Probe(t *testing.T, conn *grpc.ClientConn) (codes.Code, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := conn.Invoke(ctx, probeMethod, &emptypb.Empty{}, &emptypb.Empty{})
	return status.Code(err), err
}
