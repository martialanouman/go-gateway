package billing

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// ConfigSource resolves the reserve floor for a customer on the HOT path. It is an immutable
// snapshot read: infallible and lock-free (never a Postgres read per message at 8000/s). hasFloor=false
// means no floor applies (a soft postpaid limit — reserve is never blocked); when hasFloor is true, floor
// is the minimum the balance may reach in reserve.lua (0 strict prepaid, negative for overdraft/hard
// limit). The interface is declared consumer-side (convention §2); *ConfigProvider satisfies it.
type ConfigSource interface {
	FloorFor(customerID uuid.UUID) (hasFloor bool, floor int)
	// MOFloor returns the MO meter floor; hasFloor=false means the meter is unbounded (no floor).
	MOFloor(customerID uuid.UUID) (floor int, hasFloor bool)
}

// floorConfig is the precompiled reserve floor for one customer.
type floorConfig struct {
	hasFloor bool
	floor    int
}

// floorForCustomer maps a customer's billing config to its reserve floor (§6.9). Strict prepaid →
// floor 0. Overdraft → floor -overdraft_limit. Postpaid HARD credit limit → floor -credit_limit. Postpaid
// SOFT limit → no floor (advisory, enforced by alerting, never blocks a reserve). A limit flag set with no
// limit VALUE (overdraft_enabled without overdraft_limit, or credit_limit_is_hard without credit_limit) is
// a misconfiguration: it FAILS CLOSED to strict prepaid — the balance may never open an unbounded overdraft
// on a missing number (a nil limit is not zero, and here "unknown" must mean "most restrictive").
func floorForCustomer(c cp.BillingCustomer) floorConfig {
	switch c.BillingMode {
	case cp.BillingPostpaid:
		if c.CreditLimitIsHard {
			if c.CreditLimit == nil {
				return floorConfig{hasFloor: true, floor: 0} // hard limit with no value → strict, fail-closed
			}
			return floorConfig{hasFloor: true, floor: -*c.CreditLimit}
		}
		return floorConfig{hasFloor: false, floor: 0} // soft limit → no reserve floor
	default: // prepaid (and any unset/unknown mode → strict prepaid)
		if c.OverdraftEnabled {
			if c.OverdraftLimit == nil {
				return floorConfig{hasFloor: true, floor: 0} // overdraft on with no value → strict, fail-closed
			}
			return floorConfig{hasFloor: true, floor: -*c.OverdraftLimit}
		}
		return floorConfig{hasFloor: true, floor: 0}
	}
}

// ConfigSnapshot is an IMMUTABLE, precompiled reserve floor per customer. config-sync rebuilds it
// whole and swaps it behind an atomic pointer (like routing.Snapshot), so a hot-path read never locks and
// always sees a consistent whole. An unknown customer — one absent from the snapshot — FAILS CLOSED to
// strict prepaid (floor 0): a not-yet-known customer never gets an unconfirmed overdraft. A STALE snapshot
// (control-plane briefly unreachable) is still served in full — staleness is bounded by config-sync's
// recovery, never a mass downgrade of every customer to strict prepaid.
type ConfigSnapshot struct {
	byCustomer map[uuid.UUID]customerConfig
}

// customerConfig is a customer's precompiled billing floors: the MT reserve floor and the MO meter floor.
type customerConfig struct {
	mt      floorConfig
	moFloor *int // mo_billing_floor: how negative the MO meter may run before accrual stops; nil = no floor
}

// BuildConfigSnapshot precompiles the MT reserve floor and MO meter floor for each customer's config.
func BuildConfigSnapshot(customers []cp.BillingCustomer) *ConfigSnapshot {
	byCustomer := make(map[uuid.UUID]customerConfig, len(customers))
	for _, c := range customers {
		byCustomer[c.CustomerID] = customerConfig{mt: floorForCustomer(c), moFloor: c.MoBillingFloor}
	}
	return &ConfigSnapshot{byCustomer: byCustomer}
}

// FloorFor returns the MT reserve floor for a customer. An unknown customer, or a nil snapshot (never built
// — startup before config-sync's first push), fails closed to strict prepaid (true, 0).
func (s *ConfigSnapshot) FloorFor(customerID uuid.UUID) (hasFloor bool, floor int) {
	if s != nil {
		if c, ok := s.byCustomer[customerID]; ok {
			return c.mt.hasFloor, c.mt.floor
		}
	}
	return true, 0
}

// MOFloor returns the MO meter floor for a customer. hasFloor=false means no floor — the MO meter is
// unbounded and never stops (nil mo_billing_floor, or an unknown/absent customer). The floor is the most
// negative the meter may reach; accrual stops once the meter is at or below it. A floor of 0 is valid and
// distinct from nil: the meter starts at 0, so it is already at its floor — no MO accrues (use a negative
// floor to allow accrual down to it).
func (s *ConfigSnapshot) MOFloor(customerID uuid.UUID) (floor int, hasFloor bool) {
	if s != nil {
		if c, ok := s.byCustomer[customerID]; ok && c.moFloor != nil {
			return *c.moFloor, true
		}
	}
	return 0, false
}

// ConfigProvider holds the current ConfigSnapshot behind an atomic pointer. config-sync
// rebuilds and Stores a new snapshot whole; hot-path readers call FloorFor lock-free. The zero value is
// usable and fails closed (its snapshot is nil → strict prepaid) until the first Store.
type ConfigProvider struct {
	snap atomic.Pointer[ConfigSnapshot]
}

// Store swaps in a freshly built snapshot (config-sync, on a customers billing-config change).
func (p *ConfigProvider) Store(s *ConfigSnapshot) {
	p.snap.Store(s)
}

// FloorFor reads the current snapshot's floor for a customer, failing closed to strict prepaid when no
// snapshot has been stored yet.
func (p *ConfigProvider) FloorFor(customerID uuid.UUID) (hasFloor bool, floor int) {
	return p.snap.Load().FloorFor(customerID)
}

// MOFloor reads the current snapshot's MO meter floor for a customer (no floor when no snapshot yet).
func (p *ConfigProvider) MOFloor(customerID uuid.UUID) (floor int, hasFloor bool) {
	return p.snap.Load().MOFloor(customerID)
}

// CustomerLister loads every customer's billing configuration. *postgres.BillingRepo satisfies it;
// declared consumer-side (convention §2).
type CustomerLister interface {
	ListBillingCustomers(ctx context.Context) ([]cp.BillingCustomer, error)
}

// LoadConfigSnapshot builds a fresh snapshot from the durable billing configuration — config-sync
// calls it on startup and on each customers billing-config change, then Stores the result in the provider. On a
// load error the caller keeps serving the previous (stale) snapshot rather than a mass strict-prepaid
// downgrade: staleness is bounded by the next successful rebuild, and a control-plane blip never rejects
// legitimate overdraft/postpaid traffic.
func LoadConfigSnapshot(ctx context.Context, lister CustomerLister) (*ConfigSnapshot, error) {
	customers, err := lister.ListBillingCustomers(ctx)
	if err != nil {
		return nil, fmt.Errorf("billing: load config snapshot: %w", err)
	}
	return BuildConfigSnapshot(customers), nil
}
