// Package grpctest serves a gRPC server on a real socket and reports what a call to it answers at the
// TRANSPORT level, which is what a TLS test needs and what a mock cannot give: a handshake only happens
// over a connection.
//
// The probe deliberately calls a method no server implements. A completed handshake answers
// Unimplemented, a refused one answers Unavailable — so the probe reads the transport without knowing
// anything about the services registered above it, and the same two lines serve every binary's wiring
// test whatever it registered.
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

// probeMethod is implemented by nothing, on purpose. See the package doc.
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

// Probe runs one RPC over conn and returns its status, which carries the transport's verdict.
//
// It returns the whole error and not just its code because a refusal has to be read for its REASON: a
// connection that was reset, a peer whose certificate was refused and a peer that refused ours all
// answer Unavailable, and a test that stopped at the code would stay green on any of them.
func Probe(t *testing.T, conn *grpc.ClientConn) (codes.Code, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := conn.Invoke(ctx, probeMethod, &emptypb.Empty{}, &emptypb.Empty{})
	return status.Code(err), err
}
