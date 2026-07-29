package controlplane

import "github.com/google/uuid"

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
)

// Valid reports whether e is a known entry type.
func (e EntryType) Valid() bool {
	switch e {
	case EntryReserve, EntryCapture, EntryRelease, EntryRefund, EntryTopup, EntryAdjustment, EntryMOCharge:
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
