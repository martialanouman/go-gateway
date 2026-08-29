package router_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	"github.com/martialanouman/go-gateway/internal/router"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

type stubRates struct{ entries []cp.RateLimitEntry }

func (s stubRates) List(context.Context) ([]cp.RateLimitEntry, error) { return s.entries, nil }

func iptr(n int) *int { return &n }

type stubConns struct{}

func (stubConns) List(context.Context) ([]cp.Connector, error) { return nil, nil }

// TestRedisOutageAccountsForEveryMessage is the step-250 "zéro perte" criterion, asserted where the
// property actually lives: the router's accounting of a batch.
//
// "No message lost" is not "no message rejected" — a throttled message that leaves a rejected CDR row
// is accounted for, and the customer can see it. What the criterion forbids is a message that simply
// VANISHES: neither forwarded, nor recorded, nor retried. So the assertion is a reconciliation. Every
// submitted message_id must turn up exactly once, on exactly one side of the ledger: produced to
// mt.routed, or written to the CDR as rejected.
//
// It is deliberately counted identifier by identifier rather than as a total. A total is satisfied by
// a lost message paired with a duplicate — the two errors cancel, and the count reads clean while the
// system did the two worst things it could do at once.
//
// The outage runs through the REAL Redis-backed rate limiter, the stage whose Redis policy is
// fail-CLOSED and therefore the one most able to swallow traffic: its refusals are the ones that must
// still be recorded rather than dropped.
//
// The batch is sent TWICE, and that shape is load-bearing rather than thorough. A single batch under a
// cut Redis proves nothing about the cut: with the clock frozen, the shared bucket admits exactly the
// same `perSec` messages the per-pod ceiling would, so the assertions would hold identically against a
// healthy Redis — the test would carry "Outage" in its name and never exercise one. Draining the shared
// bucket first fixes that. Once it is empty, a second batch can only be admitted by a DIFFERENT budget,
// and the per-pod ceiling is a separate one by design ("chaque pod applique localement le plafond",
// §16). So the second batch routes `perSec` if and only if the fallback really took over.
func TestRedisOutageAccountsForEveryMessage(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)

	account, connector := uuid.New(), uuid.New()
	const perSec = 4 // the per-pod ceiling admits this many, then refuses — both outcomes must be accounted
	snap, err := ratelimit.LoadSnapshot(context.Background(),
		stubRates{[]cp.RateLimitEntry{{
			EntityType: ratelimit.EntityAccount, EntityID: account,
			Limit: cp.RateLimit{MaxPerSec: iptr(perSec)},
		}}}, stubConns{})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	frozen := time.Now() // no refill: the ceiling is a fixed budget for the whole batch
	enforcer := ratelimit.NewEnforcer(snap, ratelimit.NewLimiter(rdb, ratelimit.WithClock(func() time.Time { return frozen })))

	const batch = 12
	prod, cdr := &fakeProducer{}, &fakeCDR{}
	rec := otelrec.New(t)
	tracer := observability.Tracer(rec.Provider(), "router")
	submitted := make(map[uuid.UUID]bool, 2*batch)

	sendBatch := func() {
		t.Helper()
		records := make([]kafka.Record, batch)
		for i := range records {
			in := inbound("+2250700000000")
			in.AccountID = account
			submitted[in.MessageID] = true
			enc, err := pipeline.EncodeInbound(in)
			if err != nil {
				t.Fatalf("encode inbound: %v", err)
			}
			records[i] = enc
		}
		r := router.New(router.Deps{
			Consumer: &fakeConsumer{records: records},
			Producer: prod,
			Pipeline: pipeline.New(pipeline.Deps{
				Tracer: tracer, Resolver: stubResolver{conn: connector}, SenderIDs: allowAllSenderIDs{},
				OptOut: allowAllOptOut{}, Antispam: allowAllAntispam{}, RateLimiter: enforcer,
			}),
			CDR:    cdr,
			Tracer: tracer,
		})
		if err := r.Run(context.Background()); err != nil {
			t.Fatalf("Run must not fail the batch: a rate-limit refusal is a coded reject, "+
				"not a transport fault: %v", err)
		}
	}

	// First batch, Redis healthy: it drains the shared bucket to empty (perSec routed, the rest
	// rejected). This is both the control and the setup for the outage.
	sendBatch()
	healthyRouted := len(prod.produced)
	if healthyRouted != perSec {
		t.Fatalf("with redis up, routed %d of %d, want the configured limit %d — the control failed",
			healthyRouted, batch, perSec)
	}

	proxy.Cut()

	// Second batch, Redis gone. The shared bucket is empty and the clock is frozen, so nothing can be
	// admitted from it: every message admitted below came from the per-pod ceiling.
	sendBatch()

	// Reconcile: build the ledger of what happened to each id, and demand it match what went in.
	seen := make(map[uuid.UUID]string, batch)
	claim := func(id uuid.UUID, outcome string) {
		t.Helper()
		if prev, dup := seen[id]; dup {
			t.Errorf("message %s was accounted TWICE (%s then %s): a duplicate can mask a lost message "+
				"in any total-based count", id, prev, outcome)
		}
		seen[id] = outcome
	}
	for _, out := range prod.produced {
		routed, err := pipeline.DecodeRouted(out)
		if err != nil {
			t.Fatalf("decode routed: %v", err)
		}
		claim(routed.MessageID, "routed")
	}
	for _, row := range cdr.rows {
		if row.Status != "rejected" {
			t.Errorf("message %s wrote a %q row; under a redis outage the only recorded outcome is a "+
				"rejection", row.MessageID, row.Status)
		}
		if row.ErrorCode == nil || *row.ErrorCode == "" {
			t.Errorf("message %s was rejected with no error_code: an unexplained rejection is "+
				"indistinguishable from a drop to anyone reading the CDR", row.MessageID)
		}
		claim(row.MessageID, "rejected")
	}

	for id := range submitted {
		if _, ok := seen[id]; !ok {
			t.Errorf("message %s VANISHED during the redis outage: it was neither routed nor recorded "+
				"as rejected, and the offset was committed — this is the message loss the criterion forbids", id)
		}
	}
	for id, outcome := range seen {
		if !submitted[id] {
			t.Errorf("the router accounted for %s (%s), which was never submitted", id, outcome)
		}
	}

	// The outage half must have been served by the per-pod ceiling — a separate budget from the shared
	// bucket the first batch emptied — and that ceiling must itself still BOUND the flow. Both numbers
	// matter: routing 0 here would mean no fallback at all (or Redis never actually went away), and
	// routing all `batch` would mean the fallback stopped limiting.
	outageRouted := len(prod.produced) - healthyRouted
	if outageRouted != perSec {
		t.Errorf("routed %d of %d during the outage, want exactly the per-pod ceiling %d: the shared "+
			"bucket was already empty, so anything routed here came from the fail-closed fallback — 0 "+
			"means it never engaged, %d would mean it stopped limiting", outageRouted, batch, perSec, batch)
	}
}
