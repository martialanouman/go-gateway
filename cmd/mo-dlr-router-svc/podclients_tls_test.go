package main

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/grpctls"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/testutil/grpctest"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// deliverStub answers as if it owned the bind. What is under test is the handshake that precedes the
// call, so the answer only has to be distinguishable from a transport failure.
type deliverStub struct {
	registrypb.UnimplementedSessionRegistryServer
}

func (deliverStub) Deliver(context.Context, *registrypb.DeliverRequest) (*registrypb.DeliverResponse, error) {
	return &registrypb.DeliverResponse{Delivered: true}, nil
}

// TestThePodLegVerifiesTheDeploymentAndNotTheAddressDialled is the wiring half of the arbitration: the
// return path composes a PER-POD address, and the certificate it meets is issued PER DEPLOYMENT.
//
// The pod id is the address itself here, through a "%s" template, so the authority grpc-go derives on
// its own is a host:port no certificate of ours carries. Only the pinned name gets through — and the
// second half is what proves the fixture is not what makes the first one pass.
func TestThePodLegVerifiesTheDeploymentAndNotTheAddressDialled(t *testing.T) {
	t.Parallel()
	ca := tlstest.NewCA(t)

	// The literal, deliberately NOT smppServerIdentity: this is the name deploy/k8s gives the Deployment,
	// and what its certificate will therefore carry. Deriving it from the constant would make the test
	// follow the constant wherever it drifted, and a wrong pin would stay green.
	const deploymentName = "smpp-server-svc"
	certFile, keyFile := ca.Issue(t, deploymentName, deploymentName)
	podIdentity := config.TLS{Enabled: true, CertFile: certFile, KeyFile: keyFile, ClientCAFile: ca.CAFile}
	serverCreds, err := grpctls.ServerOption(podIdentity, silentLogger())
	if err != nil {
		t.Fatalf("ServerOption: %v", err)
	}
	srv := grpc.NewServer(serverCreds)
	registrypb.RegisterSessionRegistryServer(srv, deliverStub{})
	addr := grpctest.Serve(t, srv)

	routerCert, routerKey := ca.Issue(t, "mo-dlr-router-svc", "mo-dlr-router-svc")
	cfg := config.Config{
		SMPP: config.SMPP{PodAddrTemplate: "%s"},
		TLS:  config.TLS{Enabled: true, CertFile: routerCert, KeyFile: routerKey, ClientCAFile: ca.CAFile},
	}

	pods, err := newPodClients(cfg, silentLogger())
	if err != nil {
		t.Fatalf("newPodClients: %v", err)
	}
	if err := pods.Deliver(t.Context(), addr, "bind-1", []byte{0x01}); err != nil {
		t.Fatalf("delivery to a pod of %s was refused: %v", deploymentName, err)
	}

	// The same server at the same address, verified under the name grpc-go would have derived by itself.
	// That is what the return path does without the pin, and it reaches no pod at all.
	unpinnedCreds, err := grpctls.DialOption(cfg.TLS, silentLogger())
	if err != nil {
		t.Fatalf("DialOption: %v", err)
	}
	unpinned := modlrrouter.NewPodClients(modlrrouter.NewTemplateResolver(cfg.SMPP.PodAddrTemplate), unpinnedCreds)
	err = unpinned.Deliver(t.Context(), addr, "bind-1", []byte{0x01})
	if err == nil {
		t.Fatal("an unpinned pod leg reached the server: the test proves nothing about the pin")
	}
	if !strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("refusal not attributed to the address dialled: %v", err)
	}
}
