//go:build loadref

// This file carries the local reference run of step-201 (D2). It is behind the `loadref` build tag on
// purpose: it holds a 60-second measurement window plus warmup and settle, and `go test -race ./...`
// must not grow by two minutes because a measurement lives in the tree. Without the tag the file is not
// compiled at all, so it costs the ordinary suite nothing — not even a skipped test, which would still
// have to start the containers to decide to skip.
//
//	make load-reference

package e2e_test

import (
	"context"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/connector/breaker"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/observability/metrics"
	"github.com/martialanouman/go-gateway/internal/outcome"
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
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/test/load/bindgen"
	"github.com/martialanouman/go-gateway/test/load/gatewaymetrics"
	"github.com/martialanouman/go-gateway/test/load/steady"
)

// refDefaults are the run's shape. Every one is overridable by the env var named beside it, so a
// capacity lever can be swept without editing this file — which is the whole reason the levers of
// unit 6/7 exist.
const (
	envRate     = "REF_RATE"      // target acceptance rate, msg/s
	envWorkers  = "REF_WORKERS"   // concurrent submissions in flight
	envWarmup   = "REF_WARMUP"    // head of the injection left out of the measurement
	envMeasure  = "REF_MEASURE"   // the measurement window itself
	envSettle   = "REF_SETTLE"    // tail kept after the second reading
	envBindPool = "REF_BIND_POOL" // parallel SMPP binds to the peer
	envWindow   = "REF_WINDOW"    // SMPP window size per bind
	envCalBinds = "REF_CAL_BINDS" // binds used to calibrate the peer
	envCalHold  = "REF_CAL_HOLD"  // how long the peer calibration runs

	// The ClickHouse pool. chtest leaves both at zero, which the driver silently reads as "unset" and
	// replaces with its own 5/10 (clickhouse_options.go:412-417) — so every integration run in this
	// repository, this one included, has always used the library defaults and never the levers unit 7
	// exposed. Overriding them here is what turns the CDR write path into something this run can sweep.
	envCHMaxOpen = "REF_CH_MAX_OPEN"
	envCHMaxIdle = "REF_CH_MAX_IDLE"

	// The Kafka fetch levers. The run's BASELINE is now the production default (refKafkaConfig, pinned by
	// TestRefKafkaCarriesProductionDefaults) — before step-201d it was a struct literal, so every one of
	// these sat at zero and franz-go supplied its own instead. These sweep the baseline; they do not
	// establish it.
	envFetchMinBytes          = "REF_FETCH_MIN_BYTES"
	envFetchMaxWait           = "REF_FETCH_MAX_WAIT"
	envFetchMaxBytes          = "REF_FETCH_MAX_BYTES"
	envFetchMaxPartitionBytes = "REF_FETCH_MAX_PARTITION_BYTES"

	// envAccounts is how many SMPP accounts the run submits from. It DEFAULTS TO 1, which reproduces
	// every measurement recorded before step-201d verbatim — the whole table in test/load/README.md
	// depends on that. Raise it to spread the load across mt.inbound's partitions (D5).
	envAccounts = "REF_ACCOUNTS"
)

// lagInterval paces the backlog poll. Each poll is a broker round-trip, so it is slow — but fast enough
// that a 60-second window carries the readings [steady.Criteria.MinLagSamples] demands.
const lagInterval = 3 * time.Second

// refCriteria is D2, verbatim. PeerCeiling is left at zero and filled in by the calibration below: the
// 43 498/s of D3 was measured against the SMSC SIMULATOR, and this run's peer is the in-repo fake SMSC.
// Carrying D3's figure over would place the run under a ceiling belonging to a different peer.
func refCriteria() steady.Criteria {
	return steady.Criteria{
		MinWindow:            60 * time.Second,
		MinThroughput:        1000,
		SegmentsPerMessage:   1,
		MaxSegmentationDrift: 0.02,
		MaxLagSlopeFraction:  0.01,
		MinLagSamples:        6,
		IngestP99Budget:      250 * time.Millisecond,
	}
}

// TestReferenceRun is the D2 measurement: the whole MT path in one process — rest-api, router,
// connector-pool — against real Postgres, Kafka and ClickHouse, driven at a target rate for a full
// minute, and scored on the steady-state criteria.
//
// It is a MEASUREMENT that can fail, not a smoke test. Every figure it prints comes from a different
// owner: the acceptance rate and its p99 from the injector's own samples, the output rate from the
// gateway's submits_total (fed at the submit_sm_resp), the backlog from the broker, and the breaker
// from its Prometheus gauge.
func TestReferenceRun(t *testing.T) {
	rate := envFloat(t, envRate, 1200)
	workers := int(envFloat(t, envWorkers, 64))
	warmup := envDuration(t, envWarmup, 20*time.Second)
	measure := envDuration(t, envMeasure, 60*time.Second)
	settle := envDuration(t, envSettle, 10*time.Second)

	pool := pgtest.Pool(t)
	brokers := kafkatest.Brokers(t)
	chCfg := chtest.Config(t)
	chCfg.MaxOpenConns = int(envFloat(t, envCHMaxOpen, 10))
	chCfg.MaxIdleConns = int(envFloat(t, envCHMaxIdle, 5))
	accounts := int(envFloat(t, envAccounts, 1))
	if accounts < 1 {
		t.Fatalf("%s=%d: the run needs at least one account to submit from", envAccounts, accounts)
	}

	criteria := refCriteria()
	criteria.PeerCeiling = calibratePeer(t)

	s := buildRefStack(t, pool, brokers, chCfg, accounts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The backlog is polled for the whole injection and windowed afterwards, so the trend is fitted on
	// readings taken under load rather than on two figures either side of it.
	lags := pollLag(ctx, t, s.consumers)

	injectCfg := steady.InjectConfig{
		URL: s.rest.URL + "/v1/messages",
		// APIKey stays the first account's, so a K=1 run is byte-for-byte the run every earlier line of
		// test/load/README.md was measured on. Key is what spreads submissions across the accounts, and
		// therefore across mt.inbound's partitions (D5).
		APIKey:   s.apiKeys[0],
		Key:      func(seq uint64) string { return s.apiKeys[seq%uint64(len(s.apiKeys))] },
		Sender:   refSenderID,
		Rate:     rate,
		Workers:  workers,
		Duration: warmup + measure + settle,
	}

	// The injection runs on its own goroutine so the counters can be read from INSIDE its window. Taking
	// both readings after it returned would subtract a counter from itself and report an output of zero
	// — or, worse, an output rate over a window whose tail carried no load at all.
	var (
		rep         steady.Report
		injectErr   error
		windowStart time.Time
		started     = make(chan struct{})
		done        = make(chan struct{})
	)
	go func() {
		defer close(done)
		var once sync.Once
		rep, injectErr = steady.Inject(ctx, injectCfg, func() {
			once.Do(func() { windowStart = time.Now(); close(started) })
		})
	}()

	select {
	case <-started:
	case <-done:
		t.Fatalf("the injection ended before it began: %v", injectErr)
	}

	from, to := windowStart.Add(warmup), windowStart.Add(warmup+measure)

	// Both readings are cumulative and both are taken under load, so their difference is the window's
	// own output and the warmup's submissions stay out of it.
	submittedBefore, rejectedBefore := s.submitCountersAt(t, from)
	pipeSumBefore, pipeCountBefore := s.pipelineDurationAt(t, from)
	cpuBefore := cpuSeconds(t)

	submittedAfter, rejectedAfter := s.submitCountersAt(t, to)
	pipeSumAfter, pipeCountAfter := s.pipelineDurationAt(t, to)
	cpuAfter := cpuSeconds(t)

	<-done
	if injectErr != nil {
		t.Fatalf("inject: %v", injectErr)
	}
	win := rep.Between(from, to)

	m := steady.Measurement{
		Window:         measure,
		Accepted:       win.Accepted,
		Errors:         win.Errors,
		Submitted:      submittedAfter - submittedBefore,
		SubmitRejected: rejectedAfter - rejectedBefore,
		IngestP99:      win.P99,
		IngestSamples:  win.Samples,
		Lag:            lags.between(from, to),
		BreakerClosed:  s.breakerClosed(t),
	}
	verdict := steady.Evaluate(m, criteria)

	t.Logf("\n===== step-201 D2 reference run =====\n"+
		"target %.0f msg/s over %d workers · warmup %v · window %v · settle %v\n"+
		"injector: %d attempted, %d behind schedule (%.1f%%), first error: %v\n"+
		"ingest latency in window: p50 %v · p99 %v · max %v\n"+
		"end-to-end (gateway histogram): %s\n"+
		"backlog by topic across the window: %s\n"+
		"mt.inbound backlog by partition at window close: %s\n"+
		"router pipeline: %s\n"+
		"host: %s\n"+
		"%s\n",
		rate, workers, warmup, measure, settle,
		rep.Sent, rep.Behind, 100*float64(rep.Behind)/math.Max(1, float64(rep.Sent)), rep.FirstErr,
		win.P50.Round(time.Millisecond), win.P99.Round(time.Millisecond), win.Max.Round(time.Millisecond),
		s.e2eQuantile(t), lags.breakdown(from, to), lags.partitions(from, to),
		pipelineShare(pipeSumAfter-pipeSumBefore, pipeCountAfter-pipeCountBefore, m.Submitted, measure),
		cpuShare(cpuAfter-cpuBefore, measure),
		verdict)

	if !verdict.Pass() {
		t.Fatalf("the reference run did not hold the D2 steady state — see the verdict above")
	}
}

// calibratePeer measures what the fake SMSC absorbs, so the reference run can be placed under a figure
// belonging to THIS peer rather than under D3's, which was measured against the simulator.
//
// The rate is counted from the responses the peer produced (bindgen's Accepted), not from what the
// injector wrote: every one of them is proof the peer took a submit_sm off the wire, ran its handler and
// answered. It is a LOWER BOUND — the calibration stops at the binds it was given and never looks for
// the bend D3's sweep looks for — which is exactly what a ceiling clause needs: being comfortably under
// a lower bound is a stronger statement than being under an inferred limit.
//
// It carries the same reserve the README records against D3's own figure: it is measured with the peer
// ALONE on the host, while the reference run has it sharing those cores with the whole stack. The
// reserve is harmless at the margin actually observed — three orders of magnitude — and would matter the
// day a run came within a factor of a few of this number.
func calibratePeer(t *testing.T) float64 {
	t.Helper()
	binds := int(envFloat(t, envCalBinds, 4))
	hold := envDuration(t, envCalHold, 10*time.Second)

	// New rather than Start: Start wires the fake's logger to t.Logf, and tearing a saturated peer down
	// prints one "broken pipe" line per in-flight response — hundreds of lines burying the figures.
	peer, err := fakesmsc.New(fakesmsc.Config{})
	if err != nil {
		t.Fatalf("calibration peer: %v", err)
	}
	t.Cleanup(peer.Close)
	rep, err := bindgen.Run(context.Background(), bindgen.Config{
		Addr: peer.Addr(), Binds: binds, SystemID: "calib", Password: "pw", Hold: hold,
		Submit: &bindgen.SubmitConfig{},
	})
	if err != nil {
		t.Fatalf("calibrate peer: %v", err)
	}
	if rep.Bound != binds || rep.Dropped > 0 {
		t.Fatalf("calibration: %d of %d binds held, %d dropped: the figure would be filed under a bind count nobody ran",
			rep.Bound, binds, rep.Dropped)
	}
	if rep.Rejected > 0 {
		t.Fatalf("calibration: the peer refused %d submit_sm, so what it absorbed is not a rate it sustained",
			rep.Rejected)
	}
	if rep.Accepted == 0 {
		t.Fatal("calibration: the peer answered nothing, so it has no measured capacity at all")
	}

	ceiling := float64(rep.Accepted) / hold.Seconds()
	t.Logf("peer calibration: %d submit_sm answered over %v on %d binds = %.0f/s (a lower bound, not a bend)",
		rep.Accepted, hold, binds, ceiling)
	return ceiling
}

// refSenderID is the sender ID the run submits from; it is registered and activated in the control
// plane, so the account's strict sender-ID policy authorises the traffic instead of rejecting all of it.
const refSenderID = "LOADREF"

// refStack is the whole MT path in one process, plus the ops endpoint that exposes its catalogue.
type refStack struct {
	rest      *httptest.Server
	ops       *httptest.Server
	apiKeys   []string
	registry  *prometheus.Registry
	connector uuid.UUID
	consumers map[string]*kafka.Consumer
}

func buildRefStack(t *testing.T, pool *pgxpool.Pool, brokers []string, chCfg config.ClickHouse, accounts int) *refStack {
	t.Helper()
	apiKeys, connectorID := seedRefControlPlane(t, pool, accounts)

	kafkaCfg := refKafkaConfig(brokers)
	kafkaCfg.FetchMinBytes = int32(envFloat(t, envFetchMinBytes, float64(kafkaCfg.FetchMinBytes)))
	kafkaCfg.FetchMaxWait = envDuration(t, envFetchMaxWait, kafkaCfg.FetchMaxWait)
	kafkaCfg.FetchMaxBytes = int32(envFloat(t, envFetchMaxBytes, float64(kafkaCfg.FetchMaxBytes)))
	kafkaCfg.FetchMaxPartitionBytes = int32(envFloat(t, envFetchMaxPartitionBytes, float64(kafkaCfg.FetchMaxPartitionBytes)))

	// A no-op tracer, deliberately. The walking-skeleton test records every span to assert on them; at
	// a thousand messages a second for a minute that recorder would hold millions of spans and the run
	// would measure the harness's own memory pressure. Same trap as the fake SMSC's unbounded submit
	// log, which is why nothing here records per-message state either.
	tracer := observability.Tracer(noop.NewTracerProvider(), "loadref")

	catalog := metrics.NewCatalog()
	registry := prometheus.NewRegistry()
	registry.MustRegister(catalog.Collectors()...)
	ops := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

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

	mux, _ := restapi.New(restapi.Deps{
		Principals: postgres.NewAPIKeyRepo(pool),
		Ingestor:   ingest.NewIngestor(producer, nil),
		CDRReader:  cdrReader,
		Tracer:     tracer,
		Version:    "loadref",
	})
	restSrv := httptest.NewServer(mux)

	routerConsumer := newConsumer(t, kafkaCfg, "loadref-router", kafka.TopicMTInbound)
	acceptedConsumer := newConsumer(t, kafkaCfg, "loadref-accepted", kafka.TopicMTInbound)
	connConsumer := newConsumer(t, kafkaCfg, "loadref-connector", kafka.TopicMTRouted)

	acceptedProjector := ingest.NewAcceptedConsumer(acceptedConsumer, cdrWriter, nil, nil)

	// The outcome projection is part of the path under measurement: it is the only writer of the
	// enroute row since step-201c, so a reference run without it measures a pipeline that never
	// records what it sent.
	outcomeConsumer, err := kafka.NewConsumer(kafkaCfg, "refrun-outcome-cdr", kafka.TopicMTOutcome)
	if err != nil {
		t.Fatalf("kafka outcome consumer: %v", err)
	}
	t.Cleanup(outcomeConsumer.Close)
	outcomeProjector := outcome.NewProjector(outcomeConsumer, cdrWriter, nil, nil)

	ctx := context.Background()
	resolver, err := routing.LoadSnapshot(ctx, postgres.NewRouteRepo(pool))
	if err != nil {
		t.Fatalf("load route snapshot: %v", err)
	}
	authorizer, err := senderid.LoadSnapshot(ctx, postgres.NewAccountRepo(pool), postgres.NewSenderIDRepo(pool))
	if err != nil {
		t.Fatalf("load sender-id snapshot: %v", err)
	}
	suppressions := postgres.NewSuppressionRepo(pool)
	optSnap, err := optout.LoadSnapshot(ctx, suppressions)
	if err != nil {
		t.Fatalf("load opt-out snapshot: %v", err)
	}
	inboundIdx, err := optout.LoadInboundNumberIndex(ctx, postgres.NewInboundNumberRepo(pool))
	if err != nil {
		t.Fatalf("load inbound-number index: %v", err)
	}
	enforcer := optout.NewEnforcer(optout.NewGuard(optSnap, suppressions), inboundIdx)
	spam, err := antispam.New(ctx, postgres.NewAntispamRuleRepo(pool), nil, nil, nil)
	if err != nil {
		t.Fatalf("load anti-spam engine: %v", err)
	}

	rtr := router.New(router.Deps{
		Consumer: routerConsumer,
		Producer: producer,
		// Metrics was nil until step-201d, so pipeline_duration_seconds — the one instrument that can say
		// how much of a message's wall time is the pipeline's — was fed by no run in this repository.
		Metrics: catalog,
		Pipeline: pipeline.New(pipeline.Deps{
			Tracer:    tracer,
			Resolver:  refResolver{resolver},
			SenderIDs: authorizer,
			OptOut:    enforcer,
			Antispam:  spam,
		}),
		CDR:    cdrWriter,
		Tracer: tracer,
	})

	smsc := fakesmsc.Start(t, fakesmsc.Config{
		// RecordSubmits is deliberately OFF: the fake keeps every recorded PDU in one unbounded slice
		// behind one mutex, so a minute at a thousand a second is 60 000 retained PDUs and a lock on the
		// hot path. The output figure comes from submits_total instead, which retains nothing.
		OnSubmit: func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() },
	})

	conn := connectorpool.New(connectorpool.Deps{
		Consumer: connConsumer,
		CDR:      cdrWriter,
		// The send outcome goes to mt.outcome and a projector writes the CDR (step-201c D1). Without
		// this producer the pool's whole post-send path is a no-op, and the run would publish as its
		// reference figure the throughput of a pool that records nothing — while production pays for a
		// synchronous acked produce per message.
		Producer:    producer,
		ConnectorID: connectorID,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "gateway", Password: "pw",
			DialTimeout: 5 * time.Second, ResponseTimeout: 10 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3,
			WindowSize:   int(envFloat(t, envWindow, 64)),
			BindPoolSize: int(envFloat(t, envBindPool, 4)),
		},
		// A real per-bind breaker, fed by real submit outcomes. Only the cross-pod aggregate is stubbed
		// (that one needs Redis), which is not what D2's clause is about: it asks whether THIS connector
		// stayed healthy for the whole window.
		Breaker:      localAggregator{},
		BreakerGauge: catalog,
		Metrics:      catalog,
		Tracer:       tracer,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	start := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() { defer wg.Done(); _ = fn(runCtx) }()
	}
	start(acceptedProjector.Run)
	start(outcomeProjector.Run)
	start(rtr.Run)
	start(conn.Run)

	t.Cleanup(func() {
		cancel()
		wg.Wait()
		restSrv.Close()
		ops.Close()
		producer.Close()
		routerConsumer.Close()
		acceptedConsumer.Close()
		connConsumer.Close()
		_ = chConn.Close()
	})

	// The pool needs its binds up before the injection starts, or the warmup measures a dial.
	waitBound(t, smsc, int(envFloat(t, envBindPool, 4)))

	return &refStack{
		rest: restSrv, ops: ops, apiKeys: apiKeys, registry: registry, connector: connectorID,
		consumers: map[string]*kafka.Consumer{
			kafka.TopicMTInbound: routerConsumer,
			kafka.TopicMTRouted:  connConsumer,
			// The projection lag is the backlog step-201c introduces: it is the only writer of the
			// enroute row, so a flat mt.routed with a climbing mt.outcome is a pipeline that sends
			// without recording. D2's steady-state clause has to see it.
			kafka.TopicMTOutcome: outcomeConsumer,
		},
	}
}

// localAggregator is the in-process stand-in for the Redis breaker aggregate: it echoes each bind's own
// state back, so the pool runs REAL breakers fed by real outcomes without a Redis container.
type localAggregator struct{}

// Report echoes the reported state, which is what a single-pod aggregate would compute anyway.
func (localAggregator) Report(_ context.Context, _ string, _ int, s breaker.State) (breaker.State, error) {
	return s, nil
}

// waitBound blocks until the pool has opened all of its binds against the peer.
func waitBound(t *testing.T, smsc *fakesmsc.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if smsc.ConnCount() >= want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("only %d of the %d binds came up within 30s", smsc.ConnCount(), want)
}

func newConsumer(t *testing.T, cfg config.Kafka, group, topic string) *kafka.Consumer {
	t.Helper()
	c, err := kafka.NewConsumer(cfg, group, topic)
	if err != nil {
		t.Fatalf("kafka consumer %s: %v", group, err)
	}
	return c
}

// lagTrace is the backlog poll's output, guarded because the poller writes it while the run reads it.
//
// It keeps the per-topic breakdown beside the total. The total is what D2 scores — a backlog moving
// from mt.inbound to mt.routed is the same messages one hop along, not a queue draining — but the
// breakdown is what NAMES the slow stage, and a verdict that cannot say where the queue is leaves the
// next person to guess.
type lagTrace struct {
	mu           sync.Mutex
	samples      []steady.LagSample
	perTopic     []map[string]int64
	perPartition []map[string]map[int32]int64
}

func (l *lagTrace) add(s steady.LagSample, byTopic map[string]int64, byPartition map[string]map[int32]int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.samples = append(l.samples, s)
	l.perTopic = append(l.perTopic, byTopic)
	l.perPartition = append(l.perPartition, byPartition)
}

// breakdown renders the first and last per-topic readings inside the window, which is what separates
// "the router is behind" from "the connector is behind".
func (l *lagTrace) breakdown(from, to time.Time) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var first, last map[string]int64
	for i, s := range l.samples {
		if s.At.Before(from) || !s.At.Before(to) {
			continue
		}
		if first == nil {
			first = l.perTopic[i]
		}
		last = l.perTopic[i]
	}
	if first == nil {
		return "no reading inside the window"
	}
	out := ""
	for _, topic := range []string{kafka.TopicMTInbound, kafka.TopicMTRouted, kafka.TopicMTOutcome} {
		if out != "" {
			out += " · "
		}
		out += fmt.Sprintf("%s %d -> %d", topic, first[topic], last[topic])
	}
	return out
}

// partitions renders the LAST reading inside the window split per partition, for mt.inbound only.
//
// It answers a question the totals structurally cannot: whether the backlog is spread across the topic's
// partitions or piled on one. mt.inbound is keyed by account, so a run seeding a single account puts
// every record on one partition — and then no amount of partitions, pods or in-process shards can move
// the throughput, while the total reads exactly as it would for a balanced run (step-201d, D5/M4).
//
// A flat result after a parallelism fix means nothing until this line has been read.
func (l *lagTrace) partitions(from, to time.Time) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var last map[string]map[int32]int64
	for i, s := range l.samples {
		if s.At.Before(from) || !s.At.Before(to) {
			continue
		}
		last = l.perPartition[i]
	}
	if last == nil {
		return "no reading inside the window"
	}
	byPartition := last[kafka.TopicMTInbound]
	ids := make([]int, 0, len(byPartition))
	for p := range byPartition {
		ids = append(ids, int(p))
	}
	sort.Ints(ids)
	var b strings.Builder
	var total, loaded int64
	for _, p := range ids {
		lag := byPartition[int32(p)]
		total += lag
		if lag > 0 {
			loaded++
		}
		fmt.Fprintf(&b, "p%d=%d ", p, lag)
	}
	fmt.Fprintf(&b, "(%d of %d partitions carry a backlog, %d records)", loaded, len(ids), total)
	return b.String()
}

// between returns the readings taken inside the measurement window.
func (l *lagTrace) between(from, to time.Time) []steady.LagSample {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []steady.LagSample
	for _, s := range l.samples {
		if !s.At.Before(from) && s.At.Before(to) {
			out = append(out, s)
		}
	}
	return out
}

// pollLag samples the pipeline's total backlog until ctx is cancelled.
//
// The three consumer groups are summed rather than reported apart: what D2 asks is whether the PIPELINE is
// keeping up, and a backlog moving from mt.inbound to mt.routed is not the queue draining, it is the
// same messages one hop further along. The sum is flat only when the whole path is.
//
// A failed poll skips its tick rather than failing the run — but the tally is reported, because a run
// whose backlog could not be read for most of the window has not proved the clause, and
// [steady.Criteria.MinLagSamples] is what turns that into a failure.
func pollLag(ctx context.Context, t *testing.T, consumers map[string]*kafka.Consumer) *lagTrace {
	t.Helper()
	trace := &lagTrace{}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(lagInterval)
		defer ticker.Stop()
		var failures int
		for {
			select {
			case <-ctx.Done():
				if failures > 0 {
					t.Logf("backlog poll: %d ticks could not be read", failures)
				}
				return
			case <-ticker.C:
				at := time.Now()
				var total int64
				byTopic := make(map[string]int64, len(consumers))
				byPartition := make(map[string]map[int32]int64, len(consumers))
				ok := true
				for topic, c := range consumers {
					split, err := c.LagByPartition(ctx)
					if err != nil {
						ok = false
						break
					}
					byPartition[topic] = split[topic]
					var topicLag int64
					for _, lag := range split[topic] {
						topicLag += lag
					}
					byTopic[topic] = topicLag
					total += topicLag
				}
				if !ok {
					failures++
					continue
				}
				trace.add(steady.LagSample{At: at, Records: total}, byTopic, byPartition)
			}
		}
	}()
	t.Cleanup(wg.Wait)
	return trace
}

// cpuSeconds reports the CPU time THIS process has burned since it started, user plus system.
//
// The run stands nine components in one process — rest-api, router, two CDR projections, the connector
// pool, the fake SMSC and 64 injector workers — beside Postgres, Redpanda and ClickHouse in containers on
// the same host. Whether the router is the gateway's bottleneck or the laptop is, is not a question the
// throughput figure can answer, and every conclusion about parallelism depends on which it is
// (step-201d, D6/M3).
//
// It counts the GO PROCESS ONLY. The datastores run outside it, so the figure is a floor on what the
// host is doing, never a total.
func cpuSeconds(t *testing.T) float64 {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatalf("getrusage: %v", err)
	}
	tv := func(v syscall.Timeval) float64 { return float64(v.Sec) + float64(v.Usec)/1e6 }
	return tv(ru.Utime) + tv(ru.Stime)
}

// submitCounters reads submits_total and submit_rejected_total for this run's connector. at is the
// instant the reading is meant to represent; the counters are cumulative, so the caller subtracts two.
func (s *refStack) submitCountersAt(t *testing.T, at time.Time) (submitted, rejected uint64) {
	t.Helper()
	if wait := time.Until(at); wait > 0 {
		time.Sleep(wait)
	}
	families, err := s.registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	return uint64(sumCounter(families, "submits_total")), uint64(sumCounter(families, "submit_rejected_total"))
}

// pipelineDurationAt reads pipeline_duration_seconds' cumulative sum and count. Both are cumulative, so
// the caller subtracts two readings and divides: that quotient is the MEAN time Pipeline.Process took per
// message inside the window, exactly.
//
// It is the term that splits the run's cost in two. The consume loop is a single goroutine and its
// backlog grows for the whole window, so it is never idle: the wall time it spends per message is 1/output
// rate, and
//
//	1/rate = decode + Pipeline.Process + N x (encode + ProduceSync) + amortised commit
//
// A mean near 1/rate puts the whole cost INSIDE the pipeline; a mean far below it puts ~all of it outside,
// which names the synchronous produce. The subtraction is the measurement (step-201d, D6/M0).
//
// Before step-201d the run left router.Deps.Metrics nil, so this histogram was fed by no run at all.
func (s *refStack) pipelineDurationAt(t *testing.T, at time.Time) (sum float64, count uint64) {
	t.Helper()
	if wait := time.Until(at); wait > 0 {
		time.Sleep(wait)
	}
	families, err := s.registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != "pipeline_duration_seconds" {
			continue
		}
		for _, m := range fam.GetMetric() {
			sum += m.GetHistogram().GetSampleSum()
			count += m.GetHistogram().GetSampleCount()
		}
	}
	return sum, count
}

// breakerClosed reports whether the connector's breaker gauge reads closed. It is read from the gauge
// rather than from the breaker object: the gauge is what an operator sees, and a heartbeat that stopped
// publishing must not read as health.
func (s *refStack) breakerClosed(t *testing.T) bool {
	t.Helper()
	families, err := s.registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != "connector_breaker_state" {
			continue
		}
		for _, m := range fam.GetMetric() {
			if labelOf(m, "state") == metrics.BreakerStateClosed && m.GetGauge().GetValue() == 1 {
				return true
			}
		}
	}
	// No series at all means the heartbeat never published — which is not "closed", it is "unknown", and
	// the clause must fail on it.
	return false
}

// e2eQuantile reads the end-to-end latency histogram wired in the previous unit, through the very
// package that scores it in production. It is context beside the verdict, not a clause of it: D2 gates
// on the INGESTION budget, and this span covers the queue as well.
func (s *refStack) e2eQuantile(t *testing.T) string {
	t.Helper()
	c, err := gatewaymetrics.NewClient(s.ops.URL)
	if err != nil {
		return fmt.Sprintf("unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snap, err := c.Scrape(ctx)
	if err != nil {
		return fmt.Sprintf("unavailable: %v", err)
	}
	q, err := snap.Total().QuantileBounds(0.99)
	if err != nil {
		return fmt.Sprintf("unavailable: %v", err)
	}
	return fmt.Sprintf("%s, mean %v", q, snap.Total().Mean().Round(time.Millisecond))
}

// sumCounter totals every series of a counter family. Zero when the family is absent, which is the
// honest reading: a counter nothing ever incremented exposes no series.
func sumCounter(families []*dto.MetricFamily, name string) float64 {
	var total float64
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

func labelOf(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// seedRefControlPlane creates the customer, `accounts` REST accounts each with its own key, the sender
// ID, the connector and a catch-all route. It returns the plaintext keys and the connector id.
//
// ONE customer, ONE sender ID, ONE route, whatever the account count. That is what keeps a K=1 run and a
// K=32 run comparable: sender-ID authorisation stays two map hits, opt-out stays a Bloom miss with no
// database confirmation, and route resolution stays the same catch-all. K changes which PARTITION a
// submission lands on and nothing else — K customers or K sender IDs would change the work itself and
// make the comparison dishonest (step-201d, D5).
func seedRefControlPlane(t *testing.T, pool *pgxpool.Pool, accounts int) ([]string, uuid.UUID) {
	t.Helper()
	ctx := context.Background()

	customers := postgres.NewCustomerRepo(pool)
	accountRepo := postgres.NewAccountRepo(pool)
	credentials := postgres.NewCredentialRepo(pool)
	connectors := postgres.NewConnectorRepo(pool)
	routes := postgres.NewRouteRepo(pool)
	senderIDs := postgres.NewSenderIDRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "Reference Run"})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	sid, err := senderIDs.Create(ctx, cp.NewSenderID{CustomerID: customer.ID, Address: refSenderID})
	if err != nil {
		t.Fatalf("create sender id: %v", err)
	}
	active := cp.SenderIDActive
	if _, err := senderIDs.Update(ctx, customer.ID, sid.ID, cp.SenderIDPatch{Status: &active}); err != nil {
		t.Fatalf("activate sender id: %v", err)
	}
	keys := make([]string, 0, accounts)
	for i := range accounts {
		account, err := accountRepo.Create(ctx, cp.NewAccount{
			CustomerID: customer.ID, Name: fmt.Sprintf("loadref-app-%02d", i),
		})
		if err != nil {
			t.Fatalf("create account %d: %v", i, err)
		}
		key, hash, err := credential.GenerateAPIKey()
		if err != nil {
			t.Fatalf("generate api key %d: %v", i, err)
		}
		if _, err := credentials.Create(ctx, cp.NewCredential{
			AccountID: account.ID, Type: cp.CredentialAPIKey, APIKeyHash: &hash,
		}); err != nil {
			t.Fatalf("create credential %d: %v", i, err)
		}
		keys = append(keys, key)
	}
	connector, err := connectors.Create(ctx, cp.NewConnector{
		Name: "loadref-connector", Host: "127.0.0.1", Port: 2775,
		BindType: cp.BindTRX, SystemID: "gateway", PasswordHash: "unused-in-loadref",
	})
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}
	if _, err := routes.Create(ctx, cp.NewRoute{
		Name: "loadref-default", DistributionStrategy: cp.DistributionStatic, TargetConnectorID: &connector.ID,
	}); err != nil {
		t.Fatalf("create route: %v", err)
	}
	return keys, connector.ID
}

// refResolver adapts the declarative snapshot resolver to the enriched pipeline interface, as the
// walking skeleton does: the reference run exercises declarative routing, not the script short-cut.
type refResolver struct{ *routing.SnapshotResolver }

func (r refResolver) Resolve(ctx context.Context, req pipeline.RouteRequest) (pipeline.Route, error) {
	return r.SnapshotResolver.Resolve(ctx, req.Dest)
}

func envFloat(t *testing.T, name string, def float64) float64 {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		t.Fatalf("%s=%q is not a number: %v", name, raw, err)
	}
	return v
}

func envDuration(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("%s=%q is not a duration: %v", name, raw, err)
	}
	return v
}

// localAggregator must satisfy the pool's seam, or the breakers would silently not be wired at all.
var _ connectorpool.BreakerAggregator = localAggregator{}
