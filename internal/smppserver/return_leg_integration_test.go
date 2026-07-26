package smppserver_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

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

// stubResolver maps any pod_id to one fixed address — the in-process DeliverServer under test.
type stubResolver struct{ addr string }

func (r stubResolver) Resolve(string) (string, error) { return r.addr, nil }

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

// startDeliverServer serves the pod-local DeliverServer over the listener on an ephemeral gRPC port
// and returns its address — the address the return-path router dials after a Lookup.
func startDeliverServer(t *testing.T, l *smppserver.Listener) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen deliver server: %v", err)
	}
	srv := grpc.NewServer()
	registrypb.RegisterSessionRegistryServer(srv, smppserver.NewDeliverServer(l, discardLogger()))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
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
	smppAddr, listener := startListenerRef(t, pool, registry)
	deliverAddr := startDeliverServer(t, listener)

	pods := modlrrouter.NewPodClients(stubResolver{addr: deliverAddr})
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
	_, listener := startListenerRef(t, pool, registry)
	deliverAddr := startDeliverServer(t, listener)

	pods := modlrrouter.NewPodClients(stubResolver{addr: deliverAddr})
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
