package connectorpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/connectorpool"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
	"github.com/martialanouman/go-gateway/internal/storage/kafka"
	"github.com/martialanouman/go-gateway/internal/testutil/fakesmsc"
	"github.com/martialanouman/go-gateway/internal/testutil/otelrec"
)

// spySettler counts Release calls, so a test can assert which
// settle path a send outcome takes without a real billing service.
type spySettler struct {
	releaseCalls int
}

func (s *spySettler) Release(context.Context, pipeline.RoutedMT) { s.releaseCalls++ }

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

// TestConnectorNeverSettlesASentMessage: billing-svc settles a submitted message from mt.outcome
// (step-287d), so the send path makes no billing call — the call that cost ~173 ms a message and timed out
// four times in five. The outcome carries the reservation instead, and no billing figure the pool no
// longer knows.
func TestConnectorNeverSettlesASentMessage(t *testing.T) {
	spy := &spySettler{}
	r := billableRouted()
	sink, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, spy, nil, r)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if spy.releaseCalls != 0 {
		t.Errorf("release calls = %d, want none on the send path", spy.releaseCalls)
	}
	got := sink.outcome(t)
	if got.Status != string(clickhouse.StatusEnroute) || !got.Billable || got.OwnerType != r.OwnerType {
		t.Errorf("outcome = (status %q, billable %v, owner %q), want (enroute, true, %q)", got.Status, got.Billable, got.OwnerType, r.OwnerType)
	}
}

// TestConnectorLeavesAPermanentFailureToBilling: a permanently-rejected message is released by billing-svc
// from its failed outcome, not by the pool.
func TestConnectorLeavesAPermanentFailureToBilling(t *testing.T) {
	spy := &spySettler{}
	sink, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.SubmitFailed() }, spy, nil, billableRouted())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if spy.releaseCalls != 0 {
		t.Errorf("release calls = %d, want none on the send path", spy.releaseCalls)
	}
	if got := sink.outcome(t); got.Status != string(clickhouse.StatusFailed) || !got.Billable {
		t.Errorf("outcome = (status %q, billable %v), want (failed, true)", got.Status, got.Billable)
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
	if spy.releaseCalls != 0 {
		t.Errorf("a transient reject must not settle, got %d releases", spy.releaseCalls)
	}
}

// TestConnectorReleasesOnCancel: a message cancelled before dispatch releases its reservation (never sent).
func TestConnectorReleasesOnCancel(t *testing.T) {
	spy := &spySettler{}
	sink, err := runWithBilling(t, func(smpp.SubmitSM) fakesmsc.Resp { return fakesmsc.OK() }, spy, &fakeFlags{holder: cancel.HolderCancel}, billableRouted())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if spy.releaseCalls != 1 {
		t.Errorf("a cancelled message must release once, got %d releases", spy.releaseCalls)
	}
	if rows := sink.rows(); len(rows) != 1 || rows[0].Status != clickhouse.StatusCancelled {
		t.Errorf("expected one cancelled row, got %+v", rows)
	}
}
