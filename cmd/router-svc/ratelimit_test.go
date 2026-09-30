package main

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/grpctls"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// step-283: once a message is acknowledged, throughput can slow it but never reject it. A connector
// emptied by one account must not turn another account's messages into rejected CDRs at the router.
func TestASaturatedConnectorRejectsNoMessageAtTheRouter(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	pool := pgtest.Pool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "283-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	disabled := cp.SenderIDPolicyDisabled
	accounts := postgres.NewAccountRepo(pool)
	newAccount := func(name string) cp.Account {
		t.Helper()
		a, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: name, SenderIDPolicy: &disabled})
		if err != nil {
			t.Fatalf("create account %s: %v", name, err)
		}
		return a
	}
	big, small := newAccount("283-big"), newAccount("283-small")

	oneASecond := 1
	connectors := postgres.NewConnectorRepo(pool)
	conn, err := connectors.Create(ctx, cp.NewConnector{Name: "283-" + uuid.NewString(), Host: "h", Port: 2775,
		BindType: cp.BindTRX, SystemID: "283", Password: cp.SealedSecret{Sealed: []byte("h"), KMSKeyRef: "test/v1"},
		ThroughputLimitPerSec: &oneASecond})
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}
	t.Cleanup(func() { _ = connectors.Delete(context.Background(), conn.ID) })
	prio, dest := 1, "22507"+strconv.Itoa(int(uuid.New().ID()%9000+1000))
	routes := postgres.NewRouteRepo(pool)
	route, err := routes.Create(ctx, cp.NewRoute{
		Name: "283-" + uuid.NewString(), Priority: &prio, MatchDestPattern: &dest,
		DistributionStrategy: cp.DistributionStatic, TargetConnectorID: &conn.ID,
	})
	if err != nil {
		t.Fatalf("create route: %v", err)
	}
	t.Cleanup(func() { _ = routes.Delete(context.Background(), route.ID) })

	logger := silentLogger()
	boot, err := loadBootSnapshots(ctx, pool, logger)
	if err != nil {
		t.Fatalf("boot snapshots: %v", err)
	}
	dial, err := grpctls.Dialer(cfg.TLS, logger, "")
	if err != nil {
		t.Fatalf("dialer: %v", err)
	}
	stack, err := newPipelineStack(ctx, cfg, pool, redistest.Client(t), boot, metrics.NewCatalog(),
		observability.Tracer(nil, serviceName), logger, dial)
	if err != nil {
		t.Fatalf("newPipelineStack: %v", err)
	}
	defer stack.close()

	process := func(account cp.Account) error {
		_, _, err := stack.pipeline.Process(ctx, pipeline.InboundMT{
			MessageID: uuid.New(), TraceID: uuid.New(), AccountID: account.ID, CustomerID: customer.ID,
			From: "INFO", To: "+" + dest + "0001", Body: msg.NewBodyString("hello"), Encoding: "auto",
			SubmittedAt: time.Now(),
		})
		return err
	}
	var bigRefused []error
	for range 3 {
		if err := process(big); err != nil {
			bigRefused = append(bigRefused, err)
		}
	}
	if err := process(small); err != nil {
		t.Errorf("the small account's message was refused after the big one emptied the connector: %v — "+
			"it had already been acknowledged", err)
	}
	if len(bigRefused) > 0 {
		t.Errorf("the big account's acknowledged messages were refused: %v", bigRefused)
	}
}
