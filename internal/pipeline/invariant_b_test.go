package pipeline_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
)

// This file locks invariant (b) — one of the four green-for-life invariants (CLAUDE.md): a message
// short-circuited by the L0 exact-number match still traverses EVERY compliance stage (E.164, sender
// ID, opt-out, anti-spam, segmentation, rate). Only *route resolution* (declarative / script) is
// skipped. Each compliance stage is a spy that counts its calls, so the assertion is observable.

// countingExact is an L0 exact resolver that records how many times it was consulted (to prove that a
// message rejected by compliance never reaches route resolution).
type countingExact struct {
	hits  map[string]exact.Target
	calls atomic.Int32
}

func (c *countingExact) Resolve(_ context.Context, msisdn string) (exact.Target, bool, error) {
	c.calls.Add(1)
	t, ok := c.hits[msisdn]
	return t, ok, nil
}

// routeList is a routing.RouteLister over a fixed slice.
type routeList struct{ routes []cp.Route }

func (r routeList) List(context.Context) ([]cp.Route, error) { return r.routes, nil }

// The compliance spies: each counts its invocations. optedOut lets a test block at the opt-out stage.
type spyAuthorizer struct{ calls atomic.Int32 }

func (s *spyAuthorizer) Authorize(context.Context, uuid.UUID, uuid.UUID, string) error {
	s.calls.Add(1)
	return nil
}

type spyOptOut struct {
	calls    atomic.Int32
	optedOut bool
}

func (s *spyOptOut) IsOptedOut(context.Context, uuid.UUID, uuid.UUID, string, string) (bool, error) {
	s.calls.Add(1)
	return s.optedOut, nil
}

type spyAntispam struct{ calls atomic.Int32 }

func (s *spyAntispam) Evaluate(context.Context, uuid.UUID, uuid.UUID, string, string, []byte) (cp.AntispamAction, error) {
	s.calls.Add(1)
	return cp.AntispamAction(""), nil // empty action passes
}

type spyRate struct{ calls atomic.Int32 }

func (s *spyRate) Check(context.Context, uuid.UUID, uuid.UUID, *uuid.UUID, int) error {
	s.calls.Add(1)
	return nil
}

// l0Pipeline wires the real L0 resolver (exact hit) in front of a declarative catch-all, with counting
// compliance spies. exactConn is the L0 target; declConn is the declarative connector (which must NOT
// be selected — proving route resolution was short-circuited).
func l0Pipeline(t *testing.T, ex routing.ExactResolver, opt *spyOptOut) (*pipeline.Pipeline, uuid.UUID, *spyAuthorizer, *spyAntispam, *spyRate) {
	t.Helper()
	declConn := uuid.New()
	prefix := "225"
	decl, err := routing.LoadSnapshot(context.Background(), routeList{routes: []cp.Route{
		{ID: uuid.New(), Priority: 100, Status: cp.RouteActive, DistributionStrategy: cp.DistributionStatic,
			MatchDestPattern: &prefix, TargetConnectorID: &declConn},
	}})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	l0 := routing.NewL0Resolver(ex, nil, decl)
	sender, antispam, rate := &spyAuthorizer{}, &spyAntispam{}, &spyRate{}
	tracer := observability.Tracer(nil, "test")
	return pipeline.New(tracer, l0, sender, opt, antispam, rate, nil), declConn, sender, antispam, rate
}

// TestInvariantBExactMessageTraversesAllCompliance: a message resolved by the L0 exact short-cut still
// runs E.164, sender ID, opt-out, anti-spam, segmentation and rate — only declarative/script route
// resolution is skipped.
func TestInvariantBExactMessageTraversesAllCompliance(t *testing.T) {
	exactConn := uuid.New()
	ex := &countingExact{hits: map[string]exact.Target{
		"2250700000001": {Type: exact.TargetConnector, ID: exactConn},
	}}
	opt := &spyOptOut{}
	p, declConn, sender, antispam, rate := l0Pipeline(t, ex, opt)

	out, segs, err := p.Process(context.Background(), inbound("+2250700000001"))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Route resolution was short-circuited by L0: the exact connector is used, NOT the declarative one.
	if out.ConnectorID != exactConn {
		t.Errorf("routed to %s, want the L0 exact connector %s (declarative %s must be skipped)", out.ConnectorID, exactConn, declConn)
	}
	if ex.calls.Load() != 1 {
		t.Errorf("exact resolver consulted %d times, want 1", ex.calls.Load())
	}

	// Every compliance stage ran despite the short-cut.
	if out.To != "2250700000001" {
		t.Errorf("E.164 stage did not normalize the destination: %q", out.To)
	}
	if sender.calls.Load() != 1 {
		t.Error("sender-ID stage was skipped by the L0 short-cut")
	}
	if opt.calls.Load() != 1 {
		t.Error("opt-out stage was skipped by the L0 short-cut")
	}
	if antispam.calls.Load() != 1 {
		t.Error("anti-spam stage was skipped by the L0 short-cut")
	}
	if rate.calls.Load() != 1 {
		t.Error("rate stage was skipped by the L0 short-cut")
	}
	// segs is populated only by the segment stage (nil if skipped) — the real proof it ran.
	if len(segs) < 1 || out.SegmentCount != len(segs) {
		t.Errorf("segmentation stage was skipped: segment_count=%d segs=%d", out.SegmentCount, len(segs))
	}

	// invariant (a): the body never reaches a spy log (spies never touch it) — the body is still sealed.
}

// TestInvariantBOptOutBeatsL0: an opt-out active on an L0 (exact) number still blocks the message —
// compliance runs before route resolution, so the short-cut cannot bypass it, and the exact resolver
// is never even consulted.
func TestInvariantBOptOutBeatsL0(t *testing.T) {
	exactConn := uuid.New()
	ex := &countingExact{hits: map[string]exact.Target{
		"2250700000001": {Type: exact.TargetConnector, ID: exactConn},
	}}
	opt := &spyOptOut{optedOut: true} // the recipient opted out
	p, _, _, _, _ := l0Pipeline(t, ex, opt)

	_, _, err := p.Process(context.Background(), inbound("+2250700000001"))
	if code, _ := errs.CodeOf(err); code != errs.ErrRecipientOptedOut {
		t.Fatalf("opted-out L0 message error code = %q, want recipient_opted_out — compliance was bypassed by the short-cut", code)
	}
	if ex.calls.Load() != 0 {
		t.Errorf("route resolution ran despite the opt-out block (exact consulted %d times, want 0)", ex.calls.Load())
	}
}
