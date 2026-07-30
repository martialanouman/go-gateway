package billing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// fakeCore is a billing.Core stub: it records Reserve calls and returns a programmable result, so a decorator
// test can prove whether the internal reserve was reached and what its verdict propagates as.
type fakeCore struct {
	reserveCalls, captureCalls, releaseCalls, moCalls int
	reserveErr                                        error
	reserveBal                                        int
}

func (c *fakeCore) Reserve(context.Context, billing.Owner, uuid.UUID, int) (int, error) {
	c.reserveCalls++
	return c.reserveBal, c.reserveErr
}
func (c *fakeCore) Capture(context.Context, billing.Owner, uuid.UUID) (int, error) {
	c.captureCalls++
	return 0, nil
}
func (c *fakeCore) Release(context.Context, billing.Owner, uuid.UUID) error {
	c.releaseCalls++
	return nil
}
func (c *fakeCore) RecordMO(context.Context, billing.Owner, uuid.UUID, int) (billing.MOResult, error) {
	c.moCalls++
	return billing.MOResult{}, nil
}

type countExtMetric struct{ failOpen int }

func (m *countExtMetric) AuthzFailOpen(uuid.UUID) { m.failOpen++ }

// billerFor wires a decorator over a fake core + stub provider, with the given per-customer external config
// (nil = no provider). Returns the biller, core, provider and metric so a test can drive and assert.
func billerFor(cust uuid.UUID, ext *cp.CustomerExternalBilling) (*billing.ExternalBiller, *fakeCore, *billing.StubProvider, *countExtMetric) {
	var provider = billing.NewStubProvider()
	var cfg billing.ConfigProvider
	var externals []cp.CustomerExternalBilling
	if ext != nil {
		externals = []cp.CustomerExternalBilling{*ext}
	}
	cfg.Store(billing.BuildConfigSnapshot([]cp.BillingCustomer{{CustomerID: cust, BillingMode: cp.BillingPrepaid}}, externals))
	core := &fakeCore{}
	metric := &countExtMetric{}
	biller := billing.NewExternalBiller(core, &cfg, provider, billing.WithExternalMetric(metric))
	return biller, core, provider, metric
}

func owner(cust uuid.UUID) billing.Owner {
	return billing.Owner{Type: cp.OwnerTypeCustomer, ID: cust, CustomerID: cust}
}

// TestBillerNoProviderDelegates: a customer with no external provider reserves straight through the inner core
// with no external call.
func TestBillerNoProviderDelegates(t *testing.T) {
	cust := uuid.New()
	biller, core, provider, _ := billerFor(cust, nil)

	if _, err := biller.Reserve(context.Background(), owner(cust), uuid.New(), 3); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if core.reserveCalls != 1 {
		t.Errorf("inner Reserve calls = %d, want 1", core.reserveCalls)
	}
	if got := provider.AuthorizeCalls(uuid.Nil); got != 0 {
		t.Errorf("no external Authorize expected, got calls")
	}
}

// TestBillerBalanceCheckAllows: balance_check authorizes externally, then the internal reserve runs.
func TestBillerBalanceCheckAllows(t *testing.T) {
	cust := uuid.New()
	biller, core, provider, _ := billerFor(cust, &cp.CustomerExternalBilling{
		CustomerID: cust, ProviderID: uuid.New(), Mode: cp.ExternalModeBalanceCheck, FailurePolicy: cp.FailClosed,
	})
	provider.SetAllowed(true)
	msg := uuid.New()

	if _, err := biller.Reserve(context.Background(), owner(cust), msg, 2); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if provider.AuthorizeCalls(msg) != 1 || core.reserveCalls != 1 {
		t.Errorf("want 1 Authorize + 1 inner Reserve, got authz=%d reserve=%d", provider.AuthorizeCalls(msg), core.reserveCalls)
	}
}

// TestBillerBalanceCheckDenies: an external denial rejects with insufficient_credit and never touches the
// internal reserve (zero Lua/ledger).
func TestBillerBalanceCheckDenies(t *testing.T) {
	cust := uuid.New()
	biller, core, provider, _ := billerFor(cust, &cp.CustomerExternalBilling{
		CustomerID: cust, ProviderID: uuid.New(), Mode: cp.ExternalModeBalanceCheck, FailurePolicy: cp.FailClosed,
	})
	provider.SetAllowed(false)

	_, err := biller.Reserve(context.Background(), owner(cust), uuid.New(), 2)
	if !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve(denied) err = %v, want insufficient_credit", err)
	}
	if core.reserveCalls != 0 {
		t.Errorf("an external denial must not reach the internal reserve, got %d calls", core.reserveCalls)
	}
}

// TestBillerSyncTimeoutFailClosed: a sync authorize that times out under fail_closed refuses with
// external_billing_unavailable and does not reserve.
func TestBillerSyncTimeoutFailClosed(t *testing.T) {
	cust := uuid.New()
	timeoutMs := 5
	biller, core, provider, _ := billerFor(cust, &cp.CustomerExternalBilling{
		CustomerID: cust, ProviderID: uuid.New(), Mode: cp.ExternalModeConsumeSync,
		SyncTimeoutMs: &timeoutMs, FailurePolicy: cp.FailClosed,
	})
	provider.SetLatency(50 * time.Millisecond) // exceeds the 5ms budget

	_, err := biller.Reserve(context.Background(), owner(cust), uuid.New(), 2)
	if !errors.Is(err, errs.ErrExternalBillingUnavailable) {
		t.Fatalf("Reserve(sync timeout, fail_closed) err = %v, want external_billing_unavailable", err)
	}
	if core.reserveCalls != 0 {
		t.Errorf("fail_closed must not reserve, got %d calls", core.reserveCalls)
	}
}

// TestBillerSyncTimeoutFailOpen: the same timeout under fail_open proceeds with the internal reserve and
// increments the fail-open counter (a dead provider silently authorizing is a loud event).
func TestBillerSyncTimeoutFailOpen(t *testing.T) {
	cust := uuid.New()
	timeoutMs := 5
	biller, core, provider, metric := billerFor(cust, &cp.CustomerExternalBilling{
		CustomerID: cust, ProviderID: uuid.New(), Mode: cp.ExternalModeConsumeSync,
		SyncTimeoutMs: &timeoutMs, FailurePolicy: cp.FailOpen,
	})
	provider.SetLatency(50 * time.Millisecond)

	if _, err := biller.Reserve(context.Background(), owner(cust), uuid.New(), 2); err != nil {
		t.Fatalf("Reserve(fail_open) err = %v, want nil (proceed)", err)
	}
	if core.reserveCalls != 1 {
		t.Errorf("fail_open must still reserve internally, got %d calls", core.reserveCalls)
	}
	if metric.failOpen != 1 {
		t.Errorf("fail_open counter = %d, want 1", metric.failOpen)
	}
}

// TestBillerInternalFloorStillDenies: even when the external provider authorizes, the internal reserve's own
// verdict governs — a customer over their local floor is still denied (external is an additional gate, never a bypass).
func TestBillerInternalFloorStillDenies(t *testing.T) {
	cust := uuid.New()
	biller, core, provider, _ := billerFor(cust, &cp.CustomerExternalBilling{
		CustomerID: cust, ProviderID: uuid.New(), Mode: cp.ExternalModeBalanceCheck, FailurePolicy: cp.FailClosed,
	})
	provider.SetAllowed(true)
	core.reserveErr = errs.ErrInsufficientCredit // the internal floor rejects

	_, err := biller.Reserve(context.Background(), owner(cust), uuid.New(), 2)
	if !errors.Is(err, errs.ErrInsufficientCredit) {
		t.Fatalf("Reserve err = %v, want insufficient_credit from the internal floor", err)
	}
	if core.reserveCalls != 1 {
		t.Errorf("the internal reserve must run (it is the second gate), got %d calls", core.reserveCalls)
	}
}

// TestBillerAsyncSkipsAuthorize: consume_delegate_async reserves locally with no synchronous external call
// (confirmation is reconciled later).
func TestBillerAsyncSkipsAuthorize(t *testing.T) {
	cust := uuid.New()
	biller, core, provider, _ := billerFor(cust, &cp.CustomerExternalBilling{
		CustomerID: cust, ProviderID: uuid.New(), Mode: cp.ExternalModeConsumeAsync, FailurePolicy: cp.FailClosed,
	})
	msg := uuid.New()

	if _, err := biller.Reserve(context.Background(), owner(cust), msg, 2); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if provider.AuthorizeCalls(msg) != 0 {
		t.Errorf("async mode must not call Authorize, got %d", provider.AuthorizeCalls(msg))
	}
	if core.reserveCalls != 1 {
		t.Errorf("async mode must reserve locally, got %d calls", core.reserveCalls)
	}
}

// TestBillerDelegatesCaptureReleaseMO: capture/release/record-mo pass straight through (no external hook).
func TestBillerDelegatesCaptureReleaseMO(t *testing.T) {
	cust := uuid.New()
	biller, core, _, _ := billerFor(cust, &cp.CustomerExternalBilling{
		CustomerID: cust, ProviderID: uuid.New(), Mode: cp.ExternalModeBalanceCheck, FailurePolicy: cp.FailClosed,
	})
	ctx := context.Background()
	o := owner(cust)
	_, _ = biller.Capture(ctx, o, uuid.New())
	_ = biller.Release(ctx, o, uuid.New())
	_, _ = biller.RecordMO(ctx, o, uuid.New(), 1)
	if core.captureCalls != 1 || core.releaseCalls != 1 || core.moCalls != 1 {
		t.Errorf("delegation counts = (cap %d, rel %d, mo %d), want (1,1,1)", core.captureCalls, core.releaseCalls, core.moCalls)
	}
}
