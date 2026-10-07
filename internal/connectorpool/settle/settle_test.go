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
type countMetric struct{ releaseFailed int }

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
