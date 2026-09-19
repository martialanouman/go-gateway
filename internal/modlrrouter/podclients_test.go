package modlrrouter_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
)

// recordingDeliverServer accepts any Deliver and remembers it was reached.
type recordingDeliverServer struct {
	registrypb.UnimplementedSessionRegistryServer
	reached chan string
	name    string
}

func (s *recordingDeliverServer) Deliver(_ context.Context, _ *registrypb.DeliverRequest) (*registrypb.DeliverResponse, error) {
	s.reached <- s.name
	return &registrypb.DeliverResponse{Delivered: true}, nil
}

// startPod serves a Deliver endpoint on an ephemeral port and returns its address.
func startPod(t *testing.T, name string, reached chan string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen %s: %v", name, err)
	}
	srv := grpc.NewServer()
	registrypb.RegisterSessionRegistryServer(srv, &recordingDeliverServer{reached: reached, name: name})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// TestPodClientsDialTheAddressTheBindCarries is step-302 at the dialling end: two pods on two real
// addresses, and the delivery must land on the one whose address the registry put on the bind. Nothing
// is composed from pod_id — there is no template left to compose with, and a pod name does not resolve
// behind a Deployment, which is how the SMPP return channel went dark.
func TestPodClientsDialTheAddressTheBindCarries(t *testing.T) {
	reached := make(chan string, 2)
	addrA := startPod(t, "pod-a", reached)
	addrB := startPod(t, "pod-b", reached)

	dial := func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	pods := modlrrouter.NewPodClients(dial)
	defer pods.Close()

	// pod_id is deliberately a name that resolves nowhere: only Addr may be dialled.
	bind := modlrrouter.LiveBind{PodID: "smpp-server-svc-abcde", Addr: addrB, BindID: "b1"}
	if err := pods.Deliver(context.Background(), bind, []byte{0x01}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	select {
	case got := <-reached:
		if got != "pod-b" {
			t.Errorf("delivery reached %s, want pod-b (the address on the bind)", got)
		}
	default:
		t.Fatalf("no pod was reached; pod-a is at %s, pod-b at %s", addrA, addrB)
	}
}

// TestPodClientsSkipABindWithNoAddress pins the rollout window: a replica from before this step
// publishes no address, and its binds must be skipped as Unavailable so the caller falls back to the
// webhook — never dialled as an empty target, which would hang on a resolver error instead.
func TestPodClientsSkipABindWithNoAddress(t *testing.T) {
	dialled := 0
	pods := modlrrouter.NewPodClients(func(string) (*grpc.ClientConn, error) {
		dialled++
		return nil, fmt.Errorf("dial must not be called for a bind with no address")
	})
	defer pods.Close()

	err := pods.Deliver(context.Background(), modlrrouter.LiveBind{PodID: "p1", BindID: "b1"}, []byte{0x01})
	if err == nil {
		t.Fatal("Deliver with no address returned nil, want an Unavailable status")
	}
	if dialled != 0 {
		t.Errorf("dial called %d times for a bind with no address, want 0", dialled)
	}
}
