package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace/noop"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	pipeenc "github.com/martialanouman/go-gateway/internal/pipeline/encoding"
	"github.com/martialanouman/go-gateway/internal/pipeline/optout"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderid"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
	"github.com/martialanouman/go-gateway/internal/platform/encoding"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/routing"
)

// This file is the pipeline half of step-201d's measurement (M1). It answers ONE falsifiable question,
// and it answers it with the SHAPE of a curve rather than with a value:
//
//	does the router's per-message cost fall as concurrency rises, or does it not?
//
//	ns/op flat as -cpu rises   -> the cost is CPU work, and it PARALLELISES. A fan-out of the consume
//	                              loop converts cores into throughput.
//	ns/op rising with -cpu     -> something in the pipeline is SHARED under a lock. Fanning the consume
//	                              loop out would be a no-op, and the lock is the thing to fix.
//	ns/op already >> the budget -> the cost is irreducible per message; only more cores move the figure.
//
// The reference run of 03/08/2026 output 892 msg/s from a single consume goroutine that was never idle,
// so its budget is 1/892 = 1.12ms of WALL time per message. Read the figures below against that.
//
// It is a DIAGNOSTIC, not a gate — the same stance as TestCDRWriteCeiling, and for the same reason: a
// threshold nobody can defend fails on a busy laptop and teaches nothing. Nothing here asserts a rate.
//
//	go test -run '^$' -bench 'Pipeline' -benchmem -cpu 1,2,4,8,16 ./internal/pipeline/
//
// The collaborators are the REAL ones — the snapshot authorizer with its maps, the Bloom opt-out
// enforcer, the anti-spam engine, the declarative route snapshot — seeded from in-memory listers. In
// production they are immutable snapshots too, so this is the production code path, not a stand-in. The
// two stages that are NOT wired (rate limit, credit) are exactly the two the reference run leaves nil;
// wiring them here would measure Redis and gRPC, which the sub-benchmarks below cannot isolate anyway.

// benchBody is a 130-character GSM-7 body: one segment, the shape the load injector sends and the shape
// ADR-0012's compression figures were measured on. A longer body would benchmark segmentation instead of
// the pipeline.
const benchBody = "Your one time code is 424242. It expires in ten minutes. Do not share it with anyone, our staff will never ask you for it."

// benchSender mirrors the reference run: a registered, ACTIVE sender ID under an account left on the
// schema default policy, which is strict. Anything laxer would take a cheaper branch through Authorize
// than production does.
const benchSender = "LOADREF"

type benchListers struct {
	accountID, customerID, connectorID uuid.UUID
}

func (b benchListers) ListSenderIDPolicies(context.Context) ([]cp.AccountSenderIDPolicy, error) {
	return []cp.AccountSenderIDPolicy{{
		AccountID: b.accountID, CustomerID: b.customerID, Policy: cp.SenderIDStrict,
	}}, nil
}

func (b benchListers) ListActive(context.Context) ([]cp.SenderID, error) {
	return []cp.SenderID{{
		ID: uuid.New(), CustomerID: b.customerID, Address: benchSender, Status: cp.SenderIDActive,
	}}, nil
}

func (b benchListers) ListSuppressions(context.Context) ([]cp.Suppression, error) { return nil, nil }

func (b benchListers) List(context.Context) ([]cp.InboundNumber, error) { return nil, nil }

func (b benchListers) ListRoutes(context.Context) ([]cp.Route, error) {
	prefix := ""
	return []cp.Route{{
		ID: uuid.New(), Priority: 100, Status: cp.RouteActive,
		DistributionStrategy: cp.DistributionStatic,
		MatchDestPattern:     &prefix, TargetConnectorID: &b.connectorID,
	}}, nil
}

// antispamRules satisfies antispam.RuleLister with no rule at all, which is what the reference run's
// control plane seeds. Rules would change what is measured, so they belong to their own sweep.
type antispamRules struct{}

func (antispamRules) ListActive(context.Context) ([]cp.AntispamRule, error) { return nil, nil }

// routeLister adapts benchListers to routing.RouteLister, whose method is List.
type routeLister struct{ benchListers }

func (r routeLister) List(ctx context.Context) ([]cp.Route, error) { return r.ListRoutes(ctx) }

type benchStack struct {
	pipeline  *pipeline.Pipeline
	senderIDs *senderid.Authorizer
	optOut    *optout.Enforcer
	antispam  *antispam.Engine
	resolver  *routing.SnapshotResolver
	in        pipeline.InboundMT
	body      []byte
}

func newBenchStack(tb testing.TB) benchStack {
	tb.Helper()
	ctx := context.Background()
	l := benchListers{accountID: uuid.New(), customerID: uuid.New(), connectorID: uuid.New()}

	authorizer, err := senderid.LoadSnapshot(ctx, l, l)
	if err != nil {
		tb.Fatalf("sender-id snapshot: %v", err)
	}
	optSnap, err := optout.LoadSnapshot(ctx, l)
	if err != nil {
		tb.Fatalf("opt-out snapshot: %v", err)
	}
	inboundIdx, err := optout.LoadInboundNumberIndex(ctx, l)
	if err != nil {
		tb.Fatalf("inbound-number index: %v", err)
	}
	// A nil ExactChecker is right here and only here: the snapshot holds no suppression, so the Bloom
	// filters answer "no" and the confirmation is never reached. A benchmark that hit the database would
	// be measuring the database.
	enforcer := optout.NewEnforcer(optout.NewGuard(optSnap, nil), inboundIdx)
	spam, err := antispam.New(ctx, antispamRules{}, nil, nil, nil)
	if err != nil {
		tb.Fatalf("anti-spam engine: %v", err)
	}
	resolver, err := routing.LoadSnapshot(ctx, routeLister{l})
	if err != nil {
		tb.Fatalf("route snapshot: %v", err)
	}

	// A no-op tracer, deliberately: the reference run uses one too (a recorder would measure its own
	// memory pressure), so this measures the same code path that produced the 892/s.
	tracer := observability.Tracer(noop.NewTracerProvider(), "bench")

	return benchStack{
		pipeline: pipeline.New(pipeline.Deps{
			Tracer:    tracer,
			Resolver:  benchResolver{resolver},
			SenderIDs: authorizer,
			OptOut:    enforcer,
			Antispam:  spam,
		}),
		senderIDs: authorizer,
		optOut:    enforcer,
		antispam:  spam,
		resolver:  resolver,
		in: pipeline.InboundMT{
			MessageID: uuid.New(), TraceID: uuid.New(),
			AccountID: l.accountID, CustomerID: l.customerID,
			From: benchSender, To: "+2250700000000", Body: msg.NewBodyString(benchBody),
			Encoding: "auto", SubmittedAt: time.Now().UTC(),
		},
		body: []byte(benchBody),
	}
}

// benchResolver adapts the declarative snapshot to the pipeline's Resolver, as the reference run does.
type benchResolver struct{ *routing.SnapshotResolver }

func (r benchResolver) Resolve(ctx context.Context, req pipeline.RouteRequest) (pipeline.Route, error) {
	return r.SnapshotResolver.Resolve(ctx, req.Dest)
}

// BenchmarkPipelineProcess is the whole ordered pipeline, one message per iteration. Compare its ns/op
// against the 1.12ms wall budget the 03/08/2026 run leaves per message.
func BenchmarkPipelineProcess(b *testing.B) {
	s := newBenchStack(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := s.pipeline.Process(ctx, s.in); err != nil {
			b.Fatalf("Process: %v", err)
		}
	}
}

// BenchmarkPipelineProcessParallel is the SAME work under concurrency. Run it across -cpu and read the
// shape: flat ns/op means the cost parallelises, rising ns/op means a shared lock the fan-out cannot
// help with. This is the measurement that decides which fix step-201d's second gate carries.
func BenchmarkPipelineProcessParallel(b *testing.B) {
	s := newBenchStack(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, _, err := s.pipeline.Process(ctx, s.in); err != nil {
				b.Fatalf("Process: %v", err)
			}
		}
	})
}

// BenchmarkPipelineStages breaks the total down. It calls each stage's collaborator directly, with the
// arguments the pipeline passes it, so the sub-benchmarks sum to roughly BenchmarkPipelineProcess minus
// the span and struct-copy overhead. What it yields is a DISTRIBUTION — which stage owns which share —
// and that is what tells an optimisation where to go.
func BenchmarkPipelineStages(b *testing.B) {
	s := newBenchStack(b)
	ctx := context.Background()

	b.Run("e164", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := e164.Normalize(s.in.To); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sender_id", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := s.senderIDs.Authorize(ctx, s.in.AccountID, s.in.CustomerID, benchSender); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("opt_out", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.optOut.IsOptedOut(ctx, s.in.AccountID, s.in.CustomerID, benchSender, "2250700000000"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("anti_spam", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.antispam.Evaluate(ctx, s.in.AccountID, s.in.CustomerID, benchSender, "2250700000000", s.body); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("route", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.resolver.Resolve(ctx, "2250700000000"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("encoding", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			encoding.DetectAndCount("auto", nil, s.body)
		}
	})
	b.Run("segment", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			pipeenc.Split(s.in.MessageID, s.body, "gsm7")
		}
	})
}
