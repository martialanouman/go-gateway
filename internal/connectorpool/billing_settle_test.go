package connectorpool_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	"github.com/martialanouman/go-gateway/internal/connectorpool/settle"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// spySettler counts Capture/Release calls and returns a canned capture result, so a test can assert which
// settle path a send outcome takes without a real billing service.
type spySettler struct {
	captureCalls, releaseCalls int
	billed                     bool
	charged                    *int32
}

func (s *spySettler) Capture(context.Context, pipeline.RoutedMT) (bool, *int32) {
	s.captureCalls++
	return s.billed, s.charged
}

func (s *spySettler) Release(context.Context, pipeline.RoutedMT) { s.releaseCalls++ }

// failingBilling is a billing gRPC client whose Capture/Release always error, to prove the real settler
// fails open through the connector pool (a billing fault never redelivers a sent message).
type failingBilling struct{}

func (failingBilling) Capture(context.Context, *pb.CaptureRequest, ...grpc.CallOption) (*pb.CaptureResponse, error) {
	return nil, errors.New("billing-svc unavailable")
}

func (failingBilling) Release(context.Context, *pb.ReleaseRequest, ...grpc.CallOption) (*pb.ReleaseResponse, error) {
	return nil, errors.New("billing-svc unavailable")
}

// countingBilling records how many capture/release RPCs actually reached the wire, so a test can prove a
// billing-disabled message makes ZERO calls even with the real settler wired.
type countingBilling struct{ captures, releases int }

func (c *countingBilling) Capture(context.Context, *pb.CaptureRequest, ...grpc.CallOption) (*pb.CaptureResponse, error) {
	c.captures++
	return &pb.CaptureResponse{Captured: true, CreditsCharged: 1}, nil
}

func (c *countingBilling) Release(context.Context, *pb.ReleaseRequest, ...grpc.CallOption) (*pb.ReleaseResponse, error) {
	c.releases++
	return &pb.ReleaseResponse{}, nil
}

func billableRouted() pipeline.RoutedMT {
	r := routed()
	r.Billable = true
	r.OwnerType = cp.OwnerTypeCustomer
	return r
}

// runWithBilling drives one record through the connector with a settler (and optional cancel flags) wired,
// returning the CDR sink and Run's error.
func runWithBilling(t *testing.T, resp func(smpp.SubmitSM) fakesmsc.Resp, settler connectorpool.BillingSettler, flags connectorpool.CancelFlags, r pipeline.RoutedMT) (*poolSink, error) {
	t.Helper()
	smsc := fakesmsc.Start(t, fakesmsc.Config{OnSubmit: resp})
	rec, err := pipeline.EncodeRouted(r)
	if err != nil {
		t.Fatalf("encode routed: %v", err)
	}
	sink := newPoolSink()
	rrec := otelrec.New(t)
	svc := connectorpool.New(connectorpool.Deps{
		Consumer:    &fakeConsumer{records: []kafka.Record{rec}},
		CDR:         sink.cdr,
		Producer:    sink.out,
		Billing:     settler,
		CancelFlags: flags,
		Bind: connectorpool.BindConfig{
			Addr: smsc.Addr(), SystemID: "esme", Password: "pw",
			DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
			EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
		},
		Tracer: observability.Tracer(rrec.Provider(), "connector-pool"),
	})
	return sink, svc.Run(context.Background())
}

// TestConnectorCapturesOnEnroute: a sent billable message captures its reservation once and stamps
// billed/credits_charged from the capture onto the enroute CDR row.
func TestConnectorCapturesOnEnroute(t *testing.T) {
	charged := int32(3)
	spy := &spySettler{billed: true, charged: &charged}
	sink, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, spy, nil, billableRouted())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if spy.captureCalls != 1 || spy.releaseCalls != 0 {
		t.Errorf("settle calls = (capture %d, release %d), want (1, 0)", spy.captureCalls, spy.releaseCalls)
	}
	got := sink.outcome(t)
	if got.Status != string(clickhouse.StatusEnroute) {
		t.Fatalf("outcome status = %q, want enroute", got.Status)
	}
	if !got.Billed || got.CreditsCharged == nil || *got.CreditsCharged != 3 {
		t.Errorf("outcome billing = (billed %v, charged %v), want (true, &3)", got.Billed, got.CreditsCharged)
	}
}

// TestConnectorReleasesOnPermanentFailure: a permanently-rejected message releases its reservation once and
// writes a failed row (unbilled).
func TestConnectorReleasesOnPermanentFailure(t *testing.T) {
	spy := &spySettler{}
	sink, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SubmitFailed() }, spy, nil, billableRouted())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if spy.releaseCalls != 1 || spy.captureCalls != 0 {
		t.Errorf("settle calls = (capture %d, release %d), want (0, 1)", spy.captureCalls, spy.releaseCalls)
	}
	if got := sink.outcome(t); got.Status != string(clickhouse.StatusFailed) || got.Billed {
		t.Errorf("outcome = (status %q, billed %v), want (failed, false)", got.Status, got.Billed)
	}
}

// TestConnectorNoSettleOnTransientReject: a throttled reject is redelivered (Run errors), so NOTHING is
// settled — the reservation stays held for the retry.
func TestConnectorNoSettleOnTransientReject(t *testing.T) {
	spy := &spySettler{}
	_, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.Throttled() }, spy, nil, billableRouted())
	if err == nil {
		t.Fatal("a transient reject must redeliver (non-nil Run error)")
	}
	if spy.captureCalls != 0 || spy.releaseCalls != 0 {
		t.Errorf("a transient reject must not settle, got (capture %d, release %d)", spy.captureCalls, spy.releaseCalls)
	}
}

// TestConnectorReleasesOnCancel: a message cancelled before dispatch releases its reservation (never sent).
func TestConnectorReleasesOnCancel(t *testing.T) {
	spy := &spySettler{}
	sink, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, spy, &fakeFlags{holder: cancel.HolderCancel}, billableRouted())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if spy.releaseCalls != 1 || spy.captureCalls != 0 {
		t.Errorf("a cancelled message must release once, got (capture %d, release %d)", spy.captureCalls, spy.releaseCalls)
	}
	if rows := sink.rows(); len(rows) != 1 || rows[0].Status != clickhouse.StatusCancelled {
		t.Errorf("expected one cancelled row, got %+v", rows)
	}
}

// TestConnectorCaptureFailOpenCommits proves the no-error-leak invariant end-to-end: with the REAL settler
// over a billing client that always errors, a sent message still commits (Run nil) and writes an unbilled
// enroute row — a billing fault must never redeliver a sent message (which would be a duplicate SMS).
func TestConnectorCaptureFailOpenCommits(t *testing.T) {
	settler := settle.NewSettler(failingBilling{})
	sink, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, settler, nil, billableRouted())
	if err != nil {
		t.Fatalf("a billing fault must not redeliver a sent message: %v", err)
	}
	got := sink.outcome(t)
	if got.Status != string(clickhouse.StatusEnroute) {
		t.Fatalf("outcome status = %q, want enroute", got.Status)
	}
	if got.Billed || got.CreditsCharged != nil {
		t.Errorf("a fail-open capture must leave the outcome unbilled, got billed=%v charged=%v",
			got.Billed, got.CreditsCharged)
	}
}

// TestConnectorZeroBillingCallWhenNotBillable is the zero-network-call invariant at the wiring level: a sent
// message with no reservation (billing disabled) makes NO billing RPC, even though the connector always
// delegates to the settler — the settler's Billable gate short-circuits before the wire.
func TestConnectorZeroBillingCallWhenNotBillable(t *testing.T) {
	client := &countingBilling{}
	settler := settle.NewSettler(client)
	r := routed() // Billable defaults to false
	sink, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, settler, nil, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if client.captures != 0 || client.releases != 0 {
		t.Errorf("a non-billable message must make zero billing calls, got (capture %d, release %d)", client.captures, client.releases)
	}
	if got := sink.outcome(t); got.Status != string(clickhouse.StatusEnroute) || got.Billed {
		t.Errorf("outcome = (status %q, billed %v), want (enroute, false)", got.Status, got.Billed)
	}
}
