// Package e2e_test is the M2 walking-skeleton end-to-end test: it wires rest-api, router and
// connector-pool in one process against real Postgres, Kafka and ClickHouse (testcontainers) and
// the embedded fake SMSC, and drives a message through POST -> mt.inbound -> router -> mt.routed ->
// connector -> SMSC -> CDR, then reads it back with GET. It proves the architecture the milestone
// exists to prove.
package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	"github.com/martialanouman/go-gateway/internal/restapi"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/chtest"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// pipelineStages are the spans the router pipeline must emit for every message (plan §6 M2).
var pipelineStages = []string{
	"pipeline.e164", "pipeline.sender_id", "pipeline.opt_out", "pipeline.anti_spam",
	"pipeline.route", "pipeline.encoding", "pipeline.rate_limit", "pipeline.credit",
}

type stack struct {
	rest     *httptest.Server
	apiKey   string
	recorder *otelrec.Recorder
	smsc     *fakesmsc.Server
}

func TestE2EWalkingSkeleton(t *testing.T) {
	pool := pgtest.Pool(t)
	brokers := kafkatest.Brokers(t)
	chCfg := chtest.Config(t)
	smsc := fakesmsc.Start(t, fakesmsc.Config{
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() },
	})

	s := buildStack(t, pool, brokers, chCfg, smsc)

	// ---- Phase 1: happy path -----------------------------------------------------------------
	const body1 = "Your OTP is 424242"
	msg1 := submit(t, s, "+2250700000000", body1)

	// get-message must find the message (no persistent 404) and reach enroute end-to-end.
	waitFound(t, s, msg1)
	waitStatus(t, s, msg1, "enroute")

	// Every pipeline stage emitted its span, and no message body leaked into any of them.
	for _, name := range pipelineStages {
		if !s.recorder.Recorded(name) {
			t.Errorf("pipeline stage span %q not recorded", name)
		}
	}
	s.recorder.AssertNoBody(t, body1)

	// ---- Phase 2: durability — SMSC down, the 202 still comes out ---------------------------
	smsc.Close() // kill the SMSC; the connector bind dies, but ingestion is unaffected

	const body2 = "second message after smsc down"
	msg2 := submit(t, s, "+2250700000001", body2) // submit asserts 202 internally

	// The message is still accepted (durably queued); it never reaches enroute with the SMSC down.
	waitStatus(t, s, msg2, "accepted")
	ensureNever(t, s, msg2, "enroute", 2*time.Second)
}

func buildStack(t *testing.T, pool *pgxpool.Pool, brokers []string, chCfg config.ClickHouse, smsc *fakesmsc.Server) *stack {
	t.Helper()
	apiKey := seedControlPlane(t, pool)

	kafkaCfg := config.Kafka{Brokers: brokers, Timeout: 3 * time.Second}
	rec := otelrec.New(t)

	producer, err := kafka.NewProducer(kafkaCfg)
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	chConn, err := clickhouse.NewConn(chCfg)
	if err != nil {
		t.Fatalf("clickhouse conn: %v", err)
	}
	cdrWriter := clickhouse.NewCDRWriter(chConn)
	cdrReader := clickhouse.NewCDRReader(chConn)

	accepted := ingest.NewAcceptedWriter(cdrWriter, nil, 2, 64, nil)
	mux, _ := restapi.New(restapi.Deps{
		Principals: postgres.NewAPIKeyRepo(pool),
		Ingestor:   ingest.NewIngestor(producer, accepted, nil),
		CDRReader:  cdrReader,
		Tracer:     observability.Tracer(rec.Provider(), "rest-api"),
		Version:    "e2e",
	})
	restSrv := httptest.NewServer(mux)

	routerConsumer, err := kafka.NewConsumer(kafkaCfg, "e2e-router", kafka.TopicMTInbound)
	if err != nil {
		t.Fatalf("router consumer: %v", err)
	}
	resolver, err := routing.LoadSnapshot(context.Background(), postgres.NewRouteRepo(pool))
	if err != nil {
		t.Fatalf("load route snapshot: %v", err)
	}
	authorizer, err := senderid.LoadSnapshot(context.Background(), postgres.NewAccountRepo(pool), postgres.NewSenderIDRepo(pool))
	if err != nil {
		t.Fatalf("load sender-id snapshot: %v", err)
	}
	suppressions := postgres.NewSuppressionRepo(pool)
	optSnap, err := optout.LoadSnapshot(context.Background(), suppressions)
	if err != nil {
		t.Fatalf("load opt-out snapshot: %v", err)
	}
	inboundIdx, err := optout.LoadInboundNumberIndex(context.Background(), postgres.NewInboundNumberRepo(pool))
	if err != nil {
		t.Fatalf("load inbound-number index: %v", err)
	}
	enforcer := optout.NewEnforcer(optout.NewGuard(optSnap, suppressions), inboundIdx)
	spam, err := antispam.New(context.Background(), postgres.NewAntispamRuleRepo(pool), nil, nil, nil)
	if err != nil {
		t.Fatalf("load anti-spam engine: %v", err)
	}
	routerTracer := observability.Tracer(rec.Provider(), "router")
	rtr := router.New(router.Deps{
		Consumer: routerConsumer,
		Producer: producer,
		Pipeline: pipeline.New(routerTracer, declarativeResolver{resolver}, authorizer, enforcer, spam, nil, nil),
		CDR:      cdrWriter,
		Tracer:   routerTracer,
	})

	connConsumer, err := kafka.NewConsumer(kafkaCfg, "e2e-connector", kafka.TopicMTRouted)
	if err != nil {
		t.Fatalf("connector consumer: %v", err)
	}
	conn := connectorpool.New(connectorpool.Deps{
		Consumer: connConsumer,
		CDR:      cdrWriter,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "gateway", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(rec.Provider(), "connector"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	start := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() { defer wg.Done(); _ = fn(ctx) }()
	}
	start(accepted.Run)
	start(rtr.Run)
	start(conn.Run)

	t.Cleanup(func() {
		cancel()
		wg.Wait()
		restSrv.Close()
		producer.Close()
		routerConsumer.Close()
		connConsumer.Close()
		_ = chConn.Close()
	})

	return &stack{rest: restSrv, apiKey: apiKey, recorder: rec, smsc: smsc}
}

// seedControlPlane creates a customer, a REST-enabled account with an API key, a connector and a
// catch-all static route to it. It returns the plaintext API key.
func seedControlPlane(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	credentials := postgres.NewCredentialRepo(pool)
	connectors := postgres.NewConnectorRepo(pool)
	routes := postgres.NewRouteRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "E2E Customer"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "e2e-app"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	// Register and activate the sender ID the client submits from ("ACME"), so the account's default
	// strict sender-ID policy (§6.19) authorizes the MT rather than rejecting it.
	senderIDs := postgres.NewSenderIDRepo(pool)
	sid, err := senderIDs.Create(ctx, cp.NewSenderID{CustomerID: customer.ID, Address: "ACME"})
	if err != nil {
		t.Fatalf("create sender id: %v", err)
	}
	activeStatus := cp.SenderIDActive
	if _, err := senderIDs.Update(ctx, customer.ID, sid.ID, cp.SenderIDPatch{Status: &activeStatus}); err != nil {
		t.Fatalf("activate sender id: %v", err)
	}

	key, hash, err := credential.GenerateAPIKey()
	if err != nil {
		t.Fatalf("generate api key: %v", err)
	}
	if _, err := credentials.Create(ctx, cp.NewCredential{
		AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &hash,
	}); err != nil {
		t.Fatalf("create credential: %v", err)
	}

	connector, err := connectors.Create(ctx, cp.NewConnector{
		Name: "e2e-connector", Host: "127.0.0.1", Port: 2775,
		BindType: cp.BindTRX, SystemID: "gateway", PasswordHash: "unused-in-m2",
	})
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}
	if _, err := routes.Create(ctx, cp.NewRoute{
		Name: "e2e-default", DistributionStrategy: cp.DistributionStatic, TargetConnectorID: &connector.ID,
	}); err != nil {
		t.Fatalf("create route: %v", err)
	}

	return key
}

// submit posts a single message and asserts 202, returning the message id.
func submit(t *testing.T, s *stack, to, text string) string {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{"to": to, "from": "ACME", "text": text})
	req, err := http.NewRequest(http.MethodPost, s.rest.URL+"/v1/messages", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.apiKey)

	resp, err := s.rest.Client().Do(req)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status: got %d want 202", resp.StatusCode)
	}
	var body restapi.AcceptedMessage
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode 202: %v", err)
	}
	if _, err := uuid.Parse(body.ID); err != nil {
		t.Fatalf("202 id is not a uuid: %q", body.ID)
	}
	return body.ID
}

// getMessage reads a message; it returns (statusCode, status). A 404 yields ("", 404).
func getMessage(t *testing.T, s *stack, id string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, s.rest.URL+"/v1/messages/"+id, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	resp, err := s.rest.Client().Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, ""
	}
	var body restapi.Message
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	return resp.StatusCode, body.Status
}

// waitFound polls until get-message returns a message (200), proving there is no persistent 404
// window for an accepted message (§1.10).
func waitFound(t *testing.T, s *stack, id string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if code, _ := getMessage(t, s, id); code == http.StatusOK {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("get-message never returned a message for %s (persistent 404)", id)
}

// waitStatus polls until the message reaches the wanted status.
func waitStatus(t *testing.T, s *stack, id, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		code, status := getMessage(t, s, id)
		if code == http.StatusOK {
			last = status
			if status == want {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("message %s never reached %q (last seen %q)", id, want, last)
}

// ensureNever asserts the message does NOT reach the given status within the window.
func ensureNever(t *testing.T, s *stack, id, status string, window time.Duration) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if code, got := getMessage(t, s, id); code == http.StatusOK && got == status {
			t.Fatalf("message %s unexpectedly reached %q", id, status)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// declarativeResolver adapts the M2 declarative SnapshotResolver (keyed by destination) to the enriched
// pipeline.Resolver interface (RouteRequest, step-110). The e2e skeleton exercises declarative routing,
// not the L0/script short-cut, so it resolves on the request's destination alone.
type declarativeResolver struct{ *routing.SnapshotResolver }

func (d declarativeResolver) Resolve(ctx context.Context, req pipeline.RouteRequest) (pipeline.Route, error) {
	return d.SnapshotResolver.Resolve(ctx, req.Dest)
}
