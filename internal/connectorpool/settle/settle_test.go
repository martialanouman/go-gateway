package settle_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/connectorpool/settle"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
)

// fakeBilling counts Capture/Release calls and returns canned responses. The counters prove the
// zero-network-call gate; gotCapture captures the resolved owner and message id.
type fakeBilling struct {
	captureCalls, releaseCalls int
	captureResp                *pb.CaptureResponse
	captureErr, releaseErr     error
	gotCapture                 *pb.CaptureRequest
	gotRelease                 *pb.ReleaseRequest
}

func (f *fakeBilling) Capture(_ context.Context, in *pb.CaptureRequest, _ ...grpc.CallOption) (*pb.CaptureResponse, error) {
	f.captureCalls++
	f.gotCapture = in
	if f.captureErr != nil {
		return nil, f.captureErr
	}
	return f.captureResp, nil
}

func (f *fakeBilling) Release(_ context.Context, in *pb.ReleaseRequest, _ ...grpc.CallOption) (*pb.ReleaseResponse, error) {
	f.releaseCalls++
	f.gotRelease = in
	return &pb.ReleaseResponse{}, f.releaseErr
}

// countMetric records fail-open events so a test can assert alerting fires.
type countMetric struct{ captureFailed, releaseFailed int }

func (m *countMetric) CaptureFailed() { m.captureFailed++ }
func (m *countMetric) ReleaseFailed() { m.releaseFailed++ }

func billableRouted(ownerType string) pipeline.RoutedMT {
	return pipeline.RoutedMT{
		MessageID:  uuid.New(),
		CustomerID: uuid.New(),
		AccountID:  uuid.New(),
		Billable:   true,
		OwnerType:  ownerType,
	}
}

func newSettler(fake *fakeBilling, m settle.Metric) *settle.Settler {
	return settle.NewSettler(fake, settle.WithMetric(m))
}

// TestCaptureSkipsWhenNotBillable is the zero-network-call gate: a message with no reservation makes NO
// billing call and reports nothing settled.
func TestCaptureSkipsWhenNotBillable(t *testing.T) {
	fake := &fakeBilling{}
	s := newSettler(fake, &countMetric{})

	r := billableRouted(cp.OwnerTypeCustomer)
	r.Billable = false

	billed, charged := s.Capture(context.Background(), r)
	if billed || charged != nil {
		t.Errorf("Capture(!billable) = (%v, %v), want (false, nil)", billed, charged)
	}
	if fake.captureCalls != 0 {
		t.Errorf("Capture(!billable) made %d billing calls, want 0", fake.captureCalls)
	}
}

// TestCaptureSuccess fills billed + credits_charged from the capture response and resolves the customer owner.
func TestCaptureSuccess(t *testing.T) {
	fake := &fakeBilling{captureResp: &pb.CaptureResponse{Captured: true, CreditsCharged: 3}}
	s := newSettler(fake, &countMetric{})
	r := billableRouted(cp.OwnerTypeCustomer)

	billed, charged := s.Capture(context.Background(), r)
	if !billed || charged == nil || *charged != 3 {
		t.Fatalf("Capture = (%v, %v), want (true, &3)", billed, charged)
	}
	if o := fake.gotCapture.GetOwner(); o.GetOwnerType() != pb.OwnerType_OWNER_TYPE_CUSTOMER ||
		o.GetOwnerId() != r.CustomerID.String() || o.GetAccountId() != r.AccountID.String() {
		t.Errorf("owner = %+v, want customer owner keyed by customer_id", o)
	}
	if fake.gotCapture.GetMessageId() != r.MessageID.String() {
		t.Errorf("message_id = %q, want %q", fake.gotCapture.GetMessageId(), r.MessageID)
	}
}

// TestCaptureSmppAccountOwner: a capture on an account-scoped balance resolves the smpp_account owner keyed
// by account_id (the identical key the reserve used).
func TestCaptureSmppAccountOwner(t *testing.T) {
	fake := &fakeBilling{captureResp: &pb.CaptureResponse{Captured: true, CreditsCharged: 1}}
	s := newSettler(fake, &countMetric{})
	r := billableRouted(cp.OwnerTypeSMPPAccount)

	billed, charged := s.Capture(context.Background(), r)
	if !billed || charged == nil || *charged != 1 {
		t.Fatalf("Capture = (%v, %v), want (true, &1)", billed, charged)
	}
	if o := fake.gotCapture.GetOwner(); o.GetOwnerType() != pb.OwnerType_OWNER_TYPE_SMPP_ACCOUNT || o.GetOwnerId() != r.AccountID.String() {
		t.Errorf("owner = %+v, want smpp_account owner keyed by account_id", o)
	}
}

// TestCaptureYieldedWritesZero: a capture that yielded to a winning release returns credits_charged=0 —
// billed=false, but &0 (settled at zero) not nil (no settlement), so the CDR stays reconcilable.
func TestCaptureYieldedWritesZero(t *testing.T) {
	fake := &fakeBilling{captureResp: &pb.CaptureResponse{Captured: true, CreditsCharged: 0}}
	s := newSettler(fake, &countMetric{})

	billed, charged := s.Capture(context.Background(), billableRouted(cp.OwnerTypeCustomer))
	if billed {
		t.Error("billed = true, want false for a yielded capture")
	}
	if charged == nil || *charged != 0 {
		t.Errorf("credits_charged = %v, want &0 (settled at zero, not nil)", charged)
	}
}

// TestCaptureFailOpen: a billing fault never propagates — Capture reports (false, nil), counts the failure
// for alerting, and leaves the send committed (the reserve debit already charged the customer).
func TestCaptureFailOpen(t *testing.T) {
	fake := &fakeBilling{captureErr: errors.New("billing-svc unavailable")}
	m := &countMetric{}
	s := newSettler(fake, m)

	billed, charged := s.Capture(context.Background(), billableRouted(cp.OwnerTypeCustomer))
	if billed || charged != nil {
		t.Errorf("Capture(fault) = (%v, %v), want (false, nil) fail-open", billed, charged)
	}
	if m.captureFailed != 1 {
		t.Errorf("captureFailed metric = %d, want 1", m.captureFailed)
	}
}

// TestReleaseSkipsWhenNotBillable is the zero-call gate for release.
func TestReleaseSkipsWhenNotBillable(t *testing.T) {
	fake := &fakeBilling{}
	s := newSettler(fake, &countMetric{})
	r := billableRouted(cp.OwnerTypeCustomer)
	r.Billable = false

	s.Release(context.Background(), r)
	if fake.releaseCalls != 0 {
		t.Errorf("Release(!billable) made %d calls, want 0", fake.releaseCalls)
	}
}

// TestReleaseSmppAccountOwner: release resolves the smpp_account owner and calls billing once.
func TestReleaseSmppAccountOwner(t *testing.T) {
	fake := &fakeBilling{}
	s := newSettler(fake, &countMetric{})
	r := billableRouted(cp.OwnerTypeSMPPAccount)

	s.Release(context.Background(), r)
	if fake.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", fake.releaseCalls)
	}
	if o := fake.gotRelease.GetOwner(); o.GetOwnerType() != pb.OwnerType_OWNER_TYPE_SMPP_ACCOUNT || o.GetOwnerId() != r.AccountID.String() {
		t.Errorf("owner = %+v, want smpp_account owner keyed by account_id", o)
	}
}

// TestReleaseFailOpen: a release fault never propagates; it is counted for alerting.
func TestReleaseFailOpen(t *testing.T) {
	fake := &fakeBilling{releaseErr: errors.New("billing-svc unavailable")}
	m := &countMetric{}
	s := newSettler(fake, m)

	s.Release(context.Background(), billableRouted(cp.OwnerTypeCustomer)) // must not panic
	if m.releaseFailed != 1 {
		t.Errorf("releaseFailed metric = %d, want 1", m.releaseFailed)
	}
}
