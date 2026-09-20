package smppserver_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/modlrrouter"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
	smppsession "github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
	"github.com/martialanouman/go-gateway/internal/webhook"
)

// stubWebhookMiss reports no webhook, so the deliverer must land on the bind (or dead-letter).
type stubWebhookMiss struct{}

func (stubWebhookMiss) Get(context.Context, uuid.UUID, cp.WebhookEventType) (cp.Webhook, bool, error) {
	return cp.Webhook{}, false, nil
}

type stubSender struct{}

func (stubSender) Send(context.Context, cp.Webhook, webhook.Event) error { return nil }

// capturingProducer records parked dead-letters. Deliver calls Produce synchronously, so a read after
// Deliver returns is race-free.
type capturingProducer struct{ records []kafka.Record }

func (p *capturingProducer) Produce(_ context.Context, rec kafka.Record) error {
	p.records = append(p.records, rec)
	return nil
}

// reservePodAddr opens the port the pod-local Deliver server will serve on, BEFORE the Listener
// exists. The order matters and is the whole point of step-302: the pod publishes this exact address
// to the session registry at bind time, and the return path has nothing else to dial — no template, no
// name. So the address must be known before the pod binds anything.
func reservePodAddr(t *testing.T) (net.Listener, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen deliver server: %v", err)
	}
	return lis, lis.Addr().String()
}

// servePodDeliver serves the pod-local DeliverServer on the port reserved for it.
func servePodDeliver(t *testing.T, lis net.Listener, l *smppserver.Listener) {
	t.Helper()
	srv := grpc.NewServer()
	registrypb.RegisterSessionRegistryServer(srv, smppserver.NewDeliverServer(l, discardLogger()))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
}

// withPodAddr is what a pod's SMPP_POD_ADDR becomes in production wiring.
func withPodAddr(addr string) listenerOpt {
	return func(o *smppserver.Options) { o.PodAddr = addr }
}

// TestReturnLegDeliversViaLiveBind is step-048's return leg end to end through the delivery
// orchestration (modlrrouter.Deliverer): a bound transceiver is resolved via the real registry
// (Lookup → pod_id + bind_id), its owning pod is dialed (PodClients), and the deliver_sm reaches the
// ESME with its body intact — no webhook, no dead-letter.
func TestReturnLegDeliversViaLiveBind(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)

	sid, pw, accountID := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
	podLis, podAddr := reservePodAddr(t)
	smppAddr, listener := startListenerRef(t, pool, registry, withPodAddr(podAddr))
	servePodDeliver(t, podLis, listener)

	// No address is configured on the router: the ONLY way it can reach the pod is the address the
	// pod published to the registry at bind time. Before step-302 this dialled a composed name that
	// resolved nowhere, and every MO fell silently through to the webhook.
	pods := modlrrouter.NewPodClients(plainDial)
	defer pods.Close()
	prod := &capturingProducer{}
	deliverer := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   modlrrouter.NewRegistryLookup(registry),
		Pods:     pods,
		Webhooks: stubWebhookMiss{},
		Sender:   stubSender{},
		Producer: prod,
	})

	e := dialESME(t, smppAddr)
	defer e.close()
	if got := e.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("bind status = %#x, want ESME_ROK", got)
	}

	const body = "confidential mo body"
	done := make(chan error, 1)
	go func() {
		done <- deliverer.Deliver(context.Background(), modlrrouter.Delivery{
			AccountID: accountID,
			EventType: cp.WebhookEventMO,
			PDU:       deliverSMBytes(t, "22507000001", "36000", body),
		})
	}()

	ds := e.expectDeliver(t)
	if err := <-done; err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if string(ds.ShortMessage) != body || ds.DestinationAddr != "36000" {
		t.Errorf("received deliver_sm = dest %q / body %q, want 36000 / %q", ds.DestinationAddr, ds.ShortMessage, body)
	}
	if len(prod.records) != 0 {
		t.Errorf("delivered via bind must not dead-letter, parked %d", len(prod.records))
	}
}

// TestReturnLegDeadLettersWithoutBindOrWebhook proves the durable safety net: with no live bind and no
// webhook, the resolved event is parked, never lost.
func TestReturnLegDeadLettersWithoutBindOrWebhook(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)
	podLis, podAddr := reservePodAddr(t)
	_, listener := startListenerRef(t, pool, registry, withPodAddr(podAddr))
	servePodDeliver(t, podLis, listener)

	pods := modlrrouter.NewPodClients(plainDial)
	defer pods.Close()
	prod := &capturingProducer{}
	deliverer := modlrrouter.NewDeliverer(modlrrouter.DelivererDeps{
		Lookup:   modlrrouter.NewRegistryLookup(registry), // an account with no live binds
		Pods:     pods,
		Webhooks: stubWebhookMiss{},
		Sender:   stubSender{},
		Producer: prod,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := deliverer.Deliver(ctx, modlrrouter.Delivery{
		AccountID:  uuid.New(),
		EventType:  cp.WebhookEventMO,
		PDU:        deliverSMBytes(t, "1", "2", "x"),
		DeadLetter: kafka.Record{Topic: kafka.TopicMODeadLetter, Key: []byte("k"), Value: []byte("v")},
	})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if len(prod.records) != 1 || prod.records[0].Topic != kafka.TopicMODeadLetter {
		t.Fatalf("expected one dead-letter to %s, got %v", kafka.TopicMODeadLetter, prod.records)
	}
}

// plainDial is the return leg without TLS, which is what the integration suites run.
func plainDial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
