package billing

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// ExternalProvider is the pluggable external billing system (§6.10). The ExternalBiller decorator calls
// Authorize on the reserve hot path (balance_check / consume_delegate_sync / both) for real-time
// authorization, and Usage off the critical path for periodic reconciliation. The production implementation
// is HTTP over external_billing_providers.base_url (deferred); a local stub ships for dev and tests. The
// interface is declared consumer-side (convention §2) and never sees the message body (invariant a) — only
// owner ids, a message id and an integer credit count. messageID is carried so a real provider can dedupe a
// redelivered reserve.
type ExternalProvider interface {
	// Authorize asks the external system whether customerID may consume credits for messageID. A genuine
	// denial is (false, nil); a transport fault or timeout is a non-nil error the decorator resolves per the
	// provider's failure_policy.
	Authorize(ctx context.Context, customerID, messageID uuid.UUID, credits int) (allowed bool, err error)
	// Usage returns the external system's recorded consumed-credit total for customerID, for the periodic
	// reconciliation that compares it against the local settled total.
	Usage(ctx context.Context, customerID uuid.UUID) (consumed int64, err error)
}

// ExternalConfig is a customer's compiled external-billing configuration, read lock-free from the config
// snapshot on the reserve hot path (§6.10). It is the decorator's dispatch input: the mode decides whether a
// synchronous Authorize runs, SyncTimeout bounds that call, FailurePolicy resolves a fault, and CacheTTL is
// carried for the future caching wrapper (not used yet — the cache is a real-provider prerequisite).
type ExternalConfig struct {
	ProviderID    uuid.UUID
	Mode          cp.ExternalBillingMode
	SyncTimeout   time.Duration
	FailurePolicy cp.BillingFailurePolicy
	CacheTTL      time.Duration
}

// StubProvider is a scriptable in-memory ExternalProvider for dev and tests: no network. It is the default
// provider wired into billing-svc until a real HTTP provider lands, so external-billing config is exercised
// end to end without an external dependency. All setters are safe for concurrent use.
type StubProvider struct {
	mu             sync.Mutex
	allowed        bool
	err            error
	latency        time.Duration
	usage          map[uuid.UUID]int64
	authorizeCalls map[uuid.UUID]int
}

// NewStubProvider returns a stub that allows every authorization and records zero usage.
func NewStubProvider() *StubProvider {
	return &StubProvider{allowed: true, usage: map[uuid.UUID]int64{}, authorizeCalls: map[uuid.UUID]int{}}
}

// SetAllowed programs the allow/deny verdict returned by Authorize.
func (p *StubProvider) SetAllowed(allowed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.allowed = allowed
}

// SetError programs Authorize to return err (simulating a provider fault). A nil clears it.
func (p *StubProvider) SetError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

// SetLatency makes Authorize block for d (honouring ctx), so a decorator timeout is testable.
func (p *StubProvider) SetLatency(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latency = d
}

// SetUsage programs the external usage total reported for customerID.
func (p *StubProvider) SetUsage(customerID uuid.UUID, consumed int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.usage[customerID] = consumed
}

// AuthorizeCalls returns how many times Authorize was called for messageID (replay assertions).
func (p *StubProvider) AuthorizeCalls(messageID uuid.UUID) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.authorizeCalls[messageID]
}

// Authorize records the call, optionally sleeps to simulate latency, then returns the programmed verdict/error.
func (p *StubProvider) Authorize(ctx context.Context, _ uuid.UUID, messageID uuid.UUID, _ int) (bool, error) {
	p.mu.Lock()
	p.authorizeCalls[messageID]++
	allowed, err, latency := p.allowed, p.err, p.latency
	p.mu.Unlock()

	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if err != nil {
		return false, err
	}
	return allowed, nil
}

// Usage returns the programmed external consumption for customerID (0 if unset).
func (p *StubProvider) Usage(_ context.Context, customerID uuid.UUID) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.usage[customerID], nil
}
