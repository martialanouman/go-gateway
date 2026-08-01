package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// EntryType is a billing_ledger.entry_type — the kind of a ledger movement (§6.9). reserve/capture/
// release track the MT send lifecycle (hold → commit → refund); mo_charge is a single MO meter debit;
// refund/topup/adjustment are settlement and operator movements. It mirrors the CHECK constraint on
// control_plane.billing_ledger.entry_type.
type EntryType string

// The billing_ledger entry types (control_plane.billing_ledger.entry_type CHECK).
const (
	EntryReserve    EntryType = "reserve"
	EntryCapture    EntryType = "capture"
	EntryRelease    EntryType = "release"
	EntryRefund     EntryType = "refund"
	EntryTopup      EntryType = "topup"
	EntryAdjustment EntryType = "adjustment"
	// EntryMOCharge is a single MO meter debit (mobile-originated return path, §6.9): credits < 0 on the
	// MO balance. Message-scoped and idempotent, unlike the manual topup/adjustment types.
	EntryMOCharge EntryType = "mo_charge"
	// EntryTransfer is one leg of an admin MT balance transfer between two owners of the same customer
	// (§6.9, step-148): a transfer writes two EntryTransfer rows (debit source / credit destination) summing
	// to zero, sharing one correlation reference. Idempotent by an admin-supplied key.
	EntryTransfer EntryType = "transfer"
)

// Valid reports whether e is a known entry type.
func (e EntryType) Valid() bool {
	switch e {
	case EntryReserve, EntryCapture, EntryRelease, EntryRefund, EntryTopup, EntryAdjustment, EntryMOCharge, EntryTransfer:
		return true
	}
	return false
}

// Billing owner types and directions mirror the balances / billing_ledger columns. owner_type is the
// resolved balance owner (chosen by the customer's BalanceScope); direction keeps MT and MO separate.
const (
	OwnerTypeCustomer    = "customer"
	OwnerTypeSMPPAccount = "smpp_account"
	BillingDirectionMT   = "mt"
	BillingDirectionMO   = "mo"
)

// BillingCustomer is a customer's MT billing configuration, read from control_plane.customers (§6.9,
// step-142d consolidation). The balances themselves live in the balances table; this is the reserve-floor
// view of a customer: prepaid/postpaid, overdraft, the soft/hard credit limit, and an optional external
// billing provider.
type BillingCustomer struct {
	CustomerID                uuid.UUID
	BillingMode               BillingMode
	OverdraftEnabled          bool
	OverdraftLimit            *int
	CreditLimit               *int
	CreditLimitIsHard         bool
	MoBillingFloor            *int // how negative the MO meter may run before accrual stops+alerts; nil = no floor
	ExternalBillingProviderID *uuid.UUID
}

// BalanceOwner names one balance holder: the (owner_type, owner_id) key of the balances table. The Admin
// billing surface (step-148) resolves a customer's owners from its BalanceScope (one customer owner, or one
// per SMPP account) and passes them to the repo for balance reads and the change-scope zero-check.
type BalanceOwner struct {
	OwnerType string
	OwnerID   uuid.UUID
}

// BalanceRow is one owner's balance in one direction, for the get-customer-balances projection (step-148).
type BalanceRow struct {
	OwnerType string
	OwnerID   uuid.UUID
	Direction string
	Credits   int
}

// LedgerRow is a stored billing_ledger row echoed back to an admin top-up/transfer (step-148): the input
// LedgerEntry plus the columns the database assigned (id, balance_after, created_at).
type LedgerRow struct {
	ID           uuid.UUID
	OwnerType    string
	OwnerID      uuid.UUID
	Direction    string
	CustomerID   uuid.UUID
	AccountID    *uuid.UUID
	MessageID    *uuid.UUID
	EntryType    EntryType
	Credits      int
	BalanceAfter int
	Reference    *string
	CreatedAt    time.Time
}

// OrphanedReservation is a reservation the settle loop never closed (step-190): a message holding a
// `reserve` claim with neither a capture nor a release. Because connector-pool settles fail-open, a
// billing outage leaves the reserve debit standing — the customer stays charged for a message that may
// never have been sent. It carries the owner the reaper must settle against (the identical key the
// reserve used) plus the signed reserve delta and the moment the money got stuck.
type OrphanedReservation struct {
	MessageID  uuid.UUID
	OwnerType  string
	OwnerID    uuid.UUID
	CustomerID uuid.UUID
	AccountID  *uuid.UUID
	Credits    int // signed reserve delta (negative)
	ReservedAt time.Time
}

// LedgerKey is the keyset cursor position for the paginated billing-ledger read (step-149): the (created_at,
// id) of the last row returned. The composite avoids dropping rows that share a created_at (the id, a UUIDv7,
// breaks the tie deterministically).
type LedgerKey struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// LedgerFilter selects and paginates a customer's billing ledger (step-149 get-billing-ledger). Direction and
// AccountID are optional filters; After is the keyset position (zero value = first page); Limit is clamped by
// the handler.
type LedgerFilter struct {
	CustomerID uuid.UUID
	Direction  *string
	AccountID  *uuid.UUID
	After      LedgerKey
	Limit      int
}

// CustomerExternalBilling is a billing-enabled customer joined to its ACTIVE external billing provider
// (§6.10). billing-svc compiles these into the config snapshot so the reserve hot path knows the mode and
// budget without a DB read. A customer with no provider, or one whose provider is disabled, is simply absent
// (external layer off — pure internal billing). SyncTimeoutMs is nil when the provider set none.
type CustomerExternalBilling struct {
	CustomerID    uuid.UUID
	ProviderID    uuid.UUID
	Mode          ExternalBillingMode
	SyncTimeoutMs *int
	FailurePolicy BillingFailurePolicy
	CacheTTLMs    int
}

// CustomerBillingScope is the router-facing view of a billing-enabled customer: its identity and the
// BalanceScope that decides the reserve owner (customer vs smpp_account). The router compiles these into
// an immutable snapshot so the credit stage can gate on membership (present = billing enabled) and resolve
// the reserve Owner WITHOUT a per-message billing round-trip (§6.9, step-145). It is deliberately narrower
// than BillingCustomer: the router needs only who to bill and against which balance, not the floors.
type CustomerBillingScope struct {
	CustomerID uuid.UUID
	Scope      BalanceScope
}

// LedgerEntry is one append-only row of the billing ledger (control_plane.billing_ledger, §6.9/§6.14).
// Credits is the SIGNED delta this entry applies to the balance; the durable balance is the running sum
// of every entry's Credits (§6.14), computed atomically when the entry is recorded (the caller does NOT
// supply the resulting balance — an absolute value would race under concurrent same-owner writes). By the
// convention an MT reserve DEBITS (Credits < 0, the balance drops); a capture CONFIRMS the already-
// reserved debit with Credits == 0 (the balance is unchanged); a release REFUNDS a failed send
// (Credits > 0); a topup/refund credits and an adjustment can go either way. This type only records what
// happened. MessageID is nil for a manual top-up/adjustment; AccountID attributes a charge on a shared
// customer pool back to the originating SMPP account.
type LedgerEntry struct {
	OwnerType  string
	OwnerID    uuid.UUID
	Direction  string
	CustomerID uuid.UUID
	AccountID  *uuid.UUID
	MessageID  *uuid.UUID
	EntryType  EntryType
	Credits    int
	Reference  *string
}
