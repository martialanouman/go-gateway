package billing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
)

// TestStubProviderAuthorize covers the scriptable stub: it allows by default, can be told to deny or error,
// and counts calls per message so a replay test can prove a redelivery does not re-consume.
func TestStubProviderAuthorize(t *testing.T) {
	ctx := context.Background()
	cust, msg := uuid.New(), uuid.New()

	p := billing.NewStubProvider()
	if allowed, err := p.Authorize(ctx, cust, msg, 3); err != nil || !allowed {
		t.Fatalf("default Authorize = (%v, %v), want (true, nil)", allowed, err)
	}
	if got := p.AuthorizeCalls(msg); got != 1 {
		t.Errorf("AuthorizeCalls(msg) = %d, want 1", got)
	}
	// A replay of the same message increments the counter (the stub records every call); a real provider
	// would dedupe, but the seam must pass messageID so it can.
	if _, err := p.Authorize(ctx, cust, msg, 3); err != nil {
		t.Fatalf("replay Authorize: %v", err)
	}
	if got := p.AuthorizeCalls(msg); got != 2 {
		t.Errorf("AuthorizeCalls after replay = %d, want 2", got)
	}

	p.SetAllowed(false)
	if allowed, err := p.Authorize(ctx, cust, uuid.New(), 1); err != nil || allowed {
		t.Errorf("Authorize after SetAllowed(false) = (%v, %v), want (false, nil)", allowed, err)
	}

	boom := errors.New("provider down")
	p.SetError(boom)
	if _, err := p.Authorize(ctx, cust, uuid.New(), 1); !errors.Is(err, boom) {
		t.Errorf("Authorize after SetError = %v, want the set error", err)
	}
}

// TestStubProviderUsage covers the reconciliation read: Usage returns the programmed external total.
func TestStubProviderUsage(t *testing.T) {
	cust := uuid.New()
	p := billing.NewStubProvider()
	p.SetUsage(cust, 42)
	if got, err := p.Usage(context.Background(), cust); err != nil || got != 42 {
		t.Errorf("Usage = (%d, %v), want (42, nil)", got, err)
	}
	// An unset customer reports 0 usage (no external consumption recorded).
	if got, _ := p.Usage(context.Background(), uuid.New()); got != 0 {
		t.Errorf("Usage(unset) = %d, want 0", got)
	}
}

// TestStubProviderLatency: the stub can simulate a slow provider so the decorator's timeout path is testable.
func TestStubProviderLatency(t *testing.T) {
	p := billing.NewStubProvider()
	p.SetLatency(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := p.Authorize(ctx, uuid.New(), uuid.New(), 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Authorize with latency past the deadline = %v, want DeadlineExceeded", err)
	}
}
