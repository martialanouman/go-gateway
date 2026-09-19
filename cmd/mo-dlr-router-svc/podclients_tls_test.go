package main

import (
	"context"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/grpctls"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/testutil/grpctest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// deliverStub answers as if it owned the bind; only the handshake preceding the call is under test.
type deliverStub struct {
	registrypb.UnimplementedSessionRegistryServer
}

func (deliverStub) Deliver(context.Context, *registrypb.DeliverRequest) (*registrypb.DeliverResponse, error) {
	return &registrypb.DeliverResponse{Delivered: true}, nil
}

// The return path composes a per-pod address while the certificate it meets is issued per Deployment.
// Through a "%s" template the pod id IS the address, so the authority grpc-go would derive on its own
// is a host:port no certificate of ours carries.
func TestThePodLegVerifiesTheDeploymentAndNotTheAddressDialled(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)

	// A literal, deliberately not smppServerIdentity: deriving it from the constant would make the test
	// follow it wherever it drifted, and a wrong pin would stay green.
	const deploymentName = "smpp-server-svc"
	certFile, keyFile := ca.Issue(t, deploymentName, deploymentName)
	podIdentity := config.TLS{Enabled: true, CertFile: certFile, KeyFile: keyFile, ClientCAFile: ca.CAFile}
	srv, err := grpctls.NewServer(podIdentity, silentLogger())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	registrypb.RegisterSessionRegistryServer(srv, deliverStub{})
	addr := grpctest.Serve(t, srv)

	routerCert, routerKey := ca.Issue(t, "mo-dlr-router-svc", "mo-dlr-router-svc")
	cfg := config.Config{
		TLS: config.TLS{Enabled: true, CertFile: routerCert, KeyFile: routerKey, ClientCAFile: ca.CAFile},
	}

	pods, err := newPodClients(cfg, silentLogger())
	if err != nil {
		t.Fatalf("newPodClients: %v", err)
	}
	defer pods.Close()
	if err := pods.Deliver(t.Context(), modlrrouter.LiveBind{PodID: "smpp-server-svc-7f9c", Addr: addr, BindID: "bind-1"}, []byte{0x01}); err != nil {
		t.Fatalf("delivery to a pod of %s was refused: %v", deploymentName, err)
	}

	// The same server verified under the name grpc-go derives by itself: what the pin replaces.
	unpinnedDial, err := grpctls.Dialer(cfg.TLS, silentLogger(), "")
	if err != nil {
		t.Fatalf("Dialer: %v", err)
	}
	unpinned := modlrrouter.NewPodClients(unpinnedDial)
	defer unpinned.Close()
	err = unpinned.Deliver(t.Context(), modlrrouter.LiveBind{PodID: "smpp-server-svc-7f9c", Addr: addr, BindID: "bind-1"}, []byte{0x01})
	if err == nil {
		t.Fatal("an unpinned pod leg reached the server: the test proves nothing about the pin")
	}
	if !strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("refusal not attributed to the address dialled: %v", err)
	}
}
