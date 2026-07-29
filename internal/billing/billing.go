// Package billing is the opt-in MT credit accounting core (plan §6.9/§13, M9). It reserves, captures and
// releases credits ATOMICALLY in Redis Lua (never a Go read-modify-write on shared balance state) and is
// idempotent by message_id (invariant c). Redis is a CACHE; PostgreSQL (via a LedgerStore) is the durable
// authority — every Redis mutation is mirrored to the append-only ledger, and a cold cache is rehydrated
// from the ledger, fail-closed. It resolves the per-customer reserve floor (prepaid/overdraft/postpaid,
// step-142b) from an immutable config snapshot, and bounds cache/durable divergence with a balance-cache
// TTL. MO accounting (step-143) lives elsewhere.
//
// It REQUIRES a non-clustered Redis/Dragonfly: the reserve/release scripts touch the balance key (by
// owner) and the reservation key (by message_id), which fall on different Cluster slots — call
// EnsureNonClustered at startup. The keys are fixed by the schema §Appendix B; a future Cluster migration
// would hash-tag both by owner (an ADR-tracked schema change).
package billing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "embed"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

//go:embed lua/reserve.lua
var reserveSrc string

//go:embed lua/capture.lua
var captureSrc string

//go:embed lua/release.lua
var releaseSrc string

//go:embed lua/recordmo.lua
var recordMOSrc string

//go:embed lua/undomo.lua
var undoMOSrc string

// directionMT / directionMO name the two separate balances (§6.9). MT is a blocking prepaid balance; MO is
// a non-blocking postpaid meter (step-143). They never share a decision path.
const (
	directionMT = cp.BillingDirectionMT
	directionMO = cp.BillingDirectionMO
)

// defaultMOSeenTTL is the first-layer MO idempotency window (the billing:mo-seen:{message_id} key). A
// redelivery within it is a cheap no-op that never touches the meter; a redelivery past it falls to the
// durable idempotency guard (which self-heals, at a tiny near-floor suppression risk). Longer = more Redis
// keys at MO volume; tune with WithMOSeenTTL.
const defaultMOSeenTTL = time.Hour

// defaultHoldTTL is how long a reservation hold survives without a capture/release. It MUST exceed the
// worst SMSC round-trip plus the connector pool's retry window, so the fast path (a live hold) is the
// norm and the slow ledger-recovery path is only hit after a genuine outage.
const defaultHoldTTL = 5 * time.Minute

// defaultBalanceCacheTTL bounds how long the balance cache lives before a reserve must rehydrate it from
// the durable authority. It exists to CAP cache/durable divergence: reserve.lua/release.lua use KEEPTTL,
// so the key expires this long after each rehydrate regardless of activity, and any drift (from a rare
// concurrent race, §step-142a review) self-heals on the next rehydrate. Rehydration is always consistent
// because RecordDurable(reserve) is synchronous — the durable balance already reflects outstanding holds.
const defaultBalanceCacheTTL = 10 * time.Minute

// LedgerStore is the durable authority (control_plane balances + billing_ledger, step-141). The interface
// is declared here, consumer-side (convention §2); *postgres.BillingRepo satisfies it.
type LedgerStore interface {
	// Balance reads the durable owner balance for a direction; found=false means no row (treat as 0).
	Balance(ctx context.Context, ownerType string, ownerID uuid.UUID, direction string) (int, bool, error)
	// RecordDurable applies the entry's signed credit delta to the durable balance and appends the ledger
	// row, in one transaction. It is IDEMPOTENT by (message_id, entry_type) across day boundaries: a replay
	// applies nothing and returns applied=false with the current balance (so the caller can undo a
	// speculative cache change); a first application returns applied=true with the new balance.
	RecordDurable(ctx context.Context, entry cp.LedgerEntry) (newBalance int, applied bool, err error)
	// LedgerEntryExists is the authoritative cross-partition idempotency guard.
	LedgerEntryExists(ctx context.Context, messageID uuid.UUID, entryType cp.EntryType) (bool, error)
	// ReserveEntry reads a message's reserve entry (signed credits, balance after) so capture/release can
	// recover the amount when the Redis hold has lapsed.
	ReserveEntry(ctx context.Context, messageID uuid.UUID) (credits, balanceAfter int, found bool, err error)
}

// Owner is the balance holder and the attribution the ledger records. Type/ID key the balance (Type is
// customer|smpp_account, chosen by the customer's balance_scope); CustomerID/AccountID attribute a charge
// on a shared pool back to the originating account (nil when not applicable).
type Owner struct {
	Type       string
	ID         uuid.UUID
	CustomerID uuid.UUID
	AccountID  *uuid.UUID
}

// Accountant is the MT billing core: it drives the Lua scripts against Redis and mirrors every movement
// to the durable LedgerStore.
type Accountant struct {
	rdb        *redis.Client
	store      LedgerStore
	config     ConfigSource
	reserve    *redis.Script
	capture    *redis.Script
	release    *redis.Script
	recordMO   *redis.Script
	undoMO     *redis.Script
	holdTTL    time.Duration
	balanceTTL time.Duration
	moSeenTTL  time.Duration
	logger     *slog.Logger
}

// strictPrepaid is the default ConfigSource: every owner reserves against a floor of 0 (strict
// prepaid, no overdraft). It keeps the Accountant safe by default until real per-customer config is wired
// in via WithConfigSource.
type strictPrepaid struct{}

func (strictPrepaid) FloorFor(uuid.UUID) (hasFloor bool, floor int) { return true, 0 }

// MOFloor: the default MO meter is unbounded (no floor) until real config is wired.
func (strictPrepaid) MOFloor(uuid.UUID) (floor int, hasFloor bool) { return 0, false }

// Option tunes an Accountant.
type Option func(*Accountant)

// WithHoldTTL overrides the reservation hold TTL.
func WithHoldTTL(d time.Duration) Option {
	return func(a *Accountant) {
		if d > 0 {
			a.holdTTL = d
		}
	}
}

// WithLogger sets the logger.
func WithLogger(l *slog.Logger) Option {
	return func(a *Accountant) {
		if l != nil {
			a.logger = l
		}
	}
}

// WithConfigSource sets the per-customer reserve-floor source (prepaid/overdraft/postpaid). Without it,
// every owner reserves against strict prepaid (floor 0). config-sync builds the source's snapshot.
func WithConfigSource(src ConfigSource) Option {
	return func(a *Accountant) {
		if src != nil {
			a.config = src
		}
	}
}

// WithBalanceCacheTTL overrides how long the balance cache lives before a reserve rehydrates it — the
// bound on cache/durable divergence (see defaultBalanceCacheTTL).
func WithBalanceCacheTTL(d time.Duration) Option {
	return func(a *Accountant) {
		if d > 0 {
			a.balanceTTL = d
		}
	}
}

// WithMOSeenTTL overrides the first-layer MO idempotency window (see defaultMOSeenTTL).
func WithMOSeenTTL(d time.Duration) Option {
	return func(a *Accountant) {
		if d > 0 {
			a.moSeenTTL = d
		}
	}
}

// New builds an Accountant over rdb (a non-clustered Redis) and the durable store.
func New(rdb *redis.Client, store LedgerStore, opts ...Option) *Accountant {
	a := &Accountant{
		rdb:        rdb,
		store:      store,
		config:     strictPrepaid{},
		reserve:    redis.NewScript(reserveSrc),
		capture:    redis.NewScript(captureSrc),
		release:    redis.NewScript(releaseSrc),
		recordMO:   redis.NewScript(recordMOSrc),
		undoMO:     redis.NewScript(undoMOSrc),
		holdTTL:    defaultHoldTTL,
		balanceTTL: defaultBalanceCacheTTL,
		moSeenTTL:  defaultMOSeenTTL,
		logger:     slog.Default(),
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

// EnsureNonClustered refuses a Redis Cluster, whose multi-slot restriction would break the reserve/release
// scripts (balance and reservation keys hash to different slots). Call it once at startup. A CLUSTER INFO
// error (a minimal Dragonfly may not implement it) is treated as "not clustered" and logged, not fatal.
func (a *Accountant) EnsureNonClustered(ctx context.Context) error {
	info, err := a.rdb.ClusterInfo(ctx).Result()
	if err != nil {
		a.logger.WarnContext(ctx, "billing: CLUSTER INFO unavailable, assuming non-clustered", "err", err)
		return nil
	}
	if strings.Contains(info, "cluster_enabled:1") {
		return errors.New("billing: requires a non-clustered Redis/Dragonfly (reserve/release use multi-slot Lua)")
	}
	return nil
}

func balanceKey(o Owner) string {
	return "billing:balance:" + directionMT + ":" + o.Type + ":" + o.ID.String()
}

func moBalanceKey(o Owner) string {
	return "billing:balance:" + directionMO + ":" + o.Type + ":" + o.ID.String()
}

func reservationKey(messageID uuid.UUID) string {
	return "billing:reservation:" + messageID.String()
}

func moSeenKey(messageID uuid.UUID) string {
	return "billing:mo-seen:" + messageID.String()
}

// Reserve holds `credits` (the message's segment count, > 0) against the owner's MT balance for
// message_id, before the SMSC send. It is idempotent by message_id: a replay returns the existing
// reservation (and rejects a divergent amount, and self-repairs a durable reserve entry a crash may have
// dropped). On a cold cache it rehydrates from the durable authority and retries; if the authority is
// unreachable it fails closed. Insufficient credit returns errs.ErrInsufficientCredit with NO ledger
// entry (§6.9). On success it mirrors the reservation to the durable ledger; if that write fails it
// COMPENSATES on an un-cancellable context (so a timed-out request still refunds the cache hold) and
// surfaces the error — the message must not send.
func (a *Accountant) Reserve(ctx context.Context, owner Owner, messageID uuid.UUID, credits int) (balanceAfter int, err error) {
	if credits <= 0 {
		return 0, fmt.Errorf("billing: reserve: credits must be positive, got %d", credits)
	}
	bkey, rkey := balanceKey(owner), reservationKey(messageID)

	// Per-customer floor: strict prepaid (floor 0), overdraft (floor -limit) or a postpaid hard limit; a
	// soft/unfloored postpaid customer reserves with has_floor=0. An unknown customer fails closed to strict
	// prepaid. Read lock-free from the immutable config snapshot (step-142b). An account-scoped balance is
	// guaranteed by the DB (customers same-table CHECK, step-142c/d) never to carry an overdraft or
	// hard limit, so a per-account floor is always safe (0 strict, or none for soft postpaid) — no override
	// needed here.
	hasFloor, floor := a.config.FloorFor(owner.CustomerID)
	floorFlag := 0
	if hasFloor {
		floorFlag = 1
	}

	for attempt := 0; attempt < 2; attempt++ {
		res, err := a.reserve.Run(ctx, a.rdb, []string{bkey, rkey}, credits, floorFlag, floor, a.holdTTL.Milliseconds()).Slice()
		if err != nil {
			return 0, fmt.Errorf("billing: reserve script: %w", err)
		}
		switch status := res[0].(string); status {
		case "reserved":
			newBalance, applied, err := a.store.RecordDurable(ctx, a.entry(owner, &messageID, cp.EntryReserve, -credits))
			if err != nil {
				// A durable error is ambiguous: a lost commit-ack looks identical to a real failure. If the
				// reserve entry IS in the ledger the commit actually succeeded — compensating would refund a
				// real debit and orphan the durable reserve. Return its recorded post-reserve balance instead
				// (never a second Balance() read that could fail and wrongly send us to compensation).
				if _, balanceAfter, found, ferr := a.store.ReserveEntry(context.WithoutCancel(ctx), messageID); ferr == nil && found {
					return balanceAfter, nil
				}
				// Genuinely not persisted (or unconfirmable): undo the speculative cache debit and fail closed.
				a.undoReserveCacheDebit(ctx, bkey, rkey, messageID)
				return 0, fmt.Errorf("billing: reserve durable: %w", err)
			}
			if !applied {
				// A replay whose original reserve is in a different day partition (past the Redis hold): the
				// durable authority already holds it, but reserve.lua just re-debited the cache and placed a
				// fresh hold. Undo that speculative debit so the cache cannot drift below the durable balance.
				a.undoReserveCacheDebit(ctx, bkey, rkey, messageID)
			}
			return newBalance, nil

		case "held":
			if reserved := toInt(res[1]); reserved != credits {
				return 0, fmt.Errorf("billing: reserve %s amount mismatch (held %d, want %d): %w",
					messageID, reserved, credits, errs.ErrConflict)
			}
			// Self-repair: a crash between reserve.lua and the durable write can leave a live hold with no
			// durable reserve entry. Write it now (idempotent) so the durable authority reflects the hold.
			_, _, found, err := a.store.ReserveEntry(ctx, messageID)
			if err != nil {
				return 0, fmt.Errorf("billing: reserve replay lookup: %w", err)
			}
			if !found {
				newBalance, _, err := a.store.RecordDurable(ctx, a.entry(owner, &messageID, cp.EntryReserve, -credits))
				if err != nil {
					return 0, fmt.Errorf("billing: reserve replay repair: %w", err)
				}
				return newBalance, nil
			}
			bal, _, err := a.store.Balance(ctx, owner.Type, owner.ID, directionMT)
			if err != nil {
				return 0, fmt.Errorf("billing: reserve replay balance: %w", err)
			}
			return bal, nil

		case "cold":
			if err := a.rehydrate(ctx, bkey, owner); err != nil {
				return 0, err // fail-closed
			}
			continue // retry with the warm cache

		case "insufficient":
			return toInt(res[1]), errs.ErrInsufficientCredit

		default:
			return 0, fmt.Errorf("billing: reserve unexpected status %q", status)
		}
	}
	return 0, fmt.Errorf("billing: reserve %s: still cold after rehydration", messageID)
}

// terminalOutcome is how a durable terminal (capture/release) resolution ended.
type terminalOutcome int

const (
	outcomeRecorded    terminalOutcome = iota // the terminal entry was written now
	outcomeAlreadyDone                        // this terminal already existed — idempotent no-op
	outcomeYielded                            // the OPPOSITE terminal already won — this one is a no-op
	outcomeNoReserve                          // no durable reserve entry to base the movement on (capture anomaly)
)

func oppositeTerminal(et cp.EntryType) cp.EntryType {
	if et == cp.EntryRelease {
		return cp.EntryCapture
	}
	return cp.EntryRelease
}

// resolveTerminal writes a message's durable terminal movement (capture or release) under the terminal
// lock, so two concurrent opposite outcomes cannot both pass their ledger checks and both write. It
// yields if the opposite terminal already won, is an idempotent no-op if this terminal already exists,
// refuses (capture only) when no durable reserve entry exists, and otherwise records the movement with
// the given signed delta.
func (a *Accountant) resolveTerminal(ctx context.Context, owner Owner, messageID uuid.UUID, entryType cp.EntryType, delta int, requireReserve bool) (terminalOutcome, error) {
	outcome := outcomeRecorded
	err := a.withTerminalLock(ctx, messageID, func(lctx context.Context) error {
		if ok, e := a.store.LedgerEntryExists(lctx, messageID, entryType); e != nil {
			return e
		} else if ok {
			outcome = outcomeAlreadyDone
			return nil
		}
		if ok, e := a.store.LedgerEntryExists(lctx, messageID, oppositeTerminal(entryType)); e != nil {
			return e
		} else if ok {
			outcome = outcomeYielded
			return nil
		}
		if requireReserve {
			if _, _, found, e := a.store.ReserveEntry(lctx, messageID); e != nil {
				return e
			} else if !found {
				outcome = outcomeNoReserve
				return nil
			}
		}
		// RecordDurable is itself idempotent by (message_id, entry_type); applied=false means a concurrent
		// attempt already recorded this terminal between our check and our write — a no-op, not an error.
		if _, applied, e := a.store.RecordDurable(lctx, a.entry(owner, &messageID, entryType, delta)); e != nil {
			return e
		} else if !applied {
			outcome = outcomeAlreadyDone
			return nil
		}
		outcome = outcomeRecorded
		return nil
	})
	return outcome, err
}

// Capture commits the reservation for message_id once the SMSC accepted the message; the balance is
// unchanged (the reserve already debited it). It returns the credits charged (for the CDR). Idempotent by
// message_id and mutually exclusive with Release: a duplicate delivery is a no-op, a capture whose hold
// lapsed recovers the amount from the ledger, a capture racing a release yields to the release (first
// durable entry wins), and a capture with no durable reserve at all is a logged invariant anomaly.
func (a *Accountant) Capture(ctx context.Context, owner Owner, messageID uuid.UUID) (creditsCharged int, err error) {
	bkey, rkey := balanceKey(owner), reservationKey(messageID)
	res, err := a.capture.Run(ctx, a.rdb, []string{rkey, bkey}).Slice()
	if err != nil {
		return 0, fmt.Errorf("billing: capture script: %w", err)
	}

	// The charged amount is the live hold's credits, or — if the hold lapsed — the reserve entry's amount.
	var reserved int
	switch status := res[0].(string); status {
	case "captured":
		reserved = toInt(res[1])
	case "no_reservation":
		credits, _, found, err := a.store.ReserveEntry(ctx, messageID)
		if err != nil {
			return 0, fmt.Errorf("billing: capture recover reserve: %w", err)
		}
		if found {
			reserved = -credits // reserve credits are negative
		}
	default:
		return 0, fmt.Errorf("billing: capture unexpected status %q", status)
	}

	outcome, err := a.resolveTerminal(ctx, owner, messageID, cp.EntryCapture, 0, true)
	if err != nil {
		return 0, fmt.Errorf("billing: capture durable: %w", err)
	}
	switch outcome {
	case outcomeRecorded, outcomeAlreadyDone:
		return reserved, nil
	case outcomeYielded:
		a.logger.WarnContext(ctx, "billing: capture after release — release wins, capture is a no-op", "message_id", messageID)
		return 0, nil
	default: // outcomeNoReserve
		a.logger.ErrorContext(ctx, "billing: capture with no durable reserve entry — invariant anomaly", "message_id", messageID)
		return 0, fmt.Errorf("billing: capture %s: no durable reserve entry: %w", messageID, errs.ErrConflict)
	}
}

// Release refunds the reservation for message_id when the message failed before it was sent. reserve.lua's
// refund is atomic on the cache when a live hold exists; a lapsed hold refunds the durable balance and
// clears any stale cache. Idempotent by message_id and mutually exclusive with Capture (a release racing a
// capture yields to the capture).
func (a *Accountant) Release(ctx context.Context, owner Owner, messageID uuid.UUID) error {
	bkey, rkey := balanceKey(owner), reservationKey(messageID)
	res, err := a.release.Run(ctx, a.rdb, []string{bkey, rkey}).Slice()
	if err != nil {
		return fmt.Errorf("billing: release script: %w", err)
	}

	var refund int         // the positive credit amount to add back durably
	var cacheRefunded bool // release.lua already added the refund back to a LIVE cache (status "released")
	switch status := res[0].(string); status {
	case "released":
		refund = toInt(res[2]) // the Lua already refunded the cache by this amount
		cacheRefunded = true
	case "cold":
		refund = toInt(res[1]) // the cache had lapsed; the Lua cleared the hold, we refund durably
	case "no_reservation":
		credits, _, found, err := a.store.ReserveEntry(ctx, messageID)
		if err != nil {
			return fmt.Errorf("billing: release recover reserve: %w", err)
		}
		if !found {
			return nil // nothing to release
		}
		refund = -credits // reserve credits are negative
		// The hold lapsed but the reserve debit may still sit in a warm cache. DEL it so a stale value can
		// never diverge — the durable balance is the authority and rehydrates correctly (delta-accurate).
		if delErr := a.rdb.Del(ctx, bkey).Err(); delErr != nil {
			a.logger.WarnContext(ctx, "billing: release could not clear the balance cache", "message_id", messageID, "err", delErr)
		}
	default:
		return fmt.Errorf("billing: release unexpected status %q", status)
	}

	outcome, err := a.resolveTerminal(ctx, owner, messageID, cp.EntryRelease, refund, false)
	if err != nil {
		return fmt.Errorf("billing: release durable: %w", err)
	}
	if outcome == outcomeYielded {
		a.logger.WarnContext(ctx, "billing: release after capture — capture wins, release is a no-op", "message_id", messageID)
		// release.lua already refunded the LIVE cache, but the durable release yielded to the winning
		// capture — the cache now shows a refund the durable authority never applied. Drop it so the next
		// reserve rehydrates from the (correctly still-debited) durable balance, not a phantom-credited cache.
		// (A concurrent reserve rehydrating from an in-flight, not-yet-committed durable write could still
		// leave a brief cache/durable skew; that residual is bounded by the balance cache's TTL — step-142b.)
		if cacheRefunded {
			if delErr := a.rdb.Del(ctx, bkey).Err(); delErr != nil {
				a.logger.WarnContext(ctx, "billing: release yield could not clear the balance cache",
					"message_id", messageID, "err", delErr)
			}
		}
	}
	return nil
}

// MOResult reports the outcome of a RecordMO. Balance is the MO meter after the call. Charged is the
// credits actually accrued (0 on a duplicate or a suppressed MO). FloorReached is true on exactly the one
// MO that drove the meter to its floor (the caller alerts once, e.g. step-184 real-time transport).
// Suppressed is true when the meter was already at its floor and this MO was NOT accrued — a visible,
// meterable revenue loss the caller should count.
type MOResult struct {
	Balance      int
	Charged      int
	FloorReached bool
	Suppressed   bool
}

// RecordMO accrues one mobile-originated message on the owner's MO meter (§6.9, step-143). The MO meter is
// a POSTPAID counter that runs negative and NEVER blocks anything — it shares no decision path with the MT
// balance. Accrual is atomic in Lua and idempotent by message_id (a short-TTL seen-key plus the durable
// idempotency guard). Accrual STOPS at mo_billing_floor: the MO that crosses accrues in full and reports
// FloorReached once; later MOs at the floor are Suppressed (not accrued). On a cold cache it rehydrates
// from the durable authority and retries; on a durable-write failure it compensates the speculative cache
// debit and returns the error.
func (a *Accountant) RecordMO(ctx context.Context, owner Owner, messageID uuid.UUID, credits int) (MOResult, error) {
	if credits <= 0 {
		return MOResult{}, fmt.Errorf("billing: record MO: credits must be positive, got %d", credits)
	}
	bkey, skey := moBalanceKey(owner), moSeenKey(messageID)
	floor, hasFloor := a.config.MOFloor(owner.CustomerID)
	floorFlag := 0
	if hasFloor {
		floorFlag = 1
	}

	for attempt := 0; attempt < 2; attempt++ {
		res, err := a.recordMO.Run(ctx, a.rdb, []string{bkey, skey}, credits, floorFlag, floor, a.moSeenTTL.Milliseconds()).Slice()
		if err != nil {
			return MOResult{}, fmt.Errorf("billing: record MO script: %w", err)
		}
		switch status := res[0].(string); status {
		case "cold":
			if err := a.rehydrateMO(ctx, bkey, owner); err != nil {
				return MOResult{}, err // fail-closed
			}
			continue // retry with the warm cache

		case "duplicate":
			// Already accrued within the seen window — a no-op. Report the current meter, nothing charged.
			return MOResult{Balance: toInt(res[1]), Charged: 0}, nil

		case "stopped":
			// The meter is already at/below the floor: this MO is dropped (accrual stopped). Surface it so
			// the caller can meter the suppressed-MO revenue loss (it is not written to the ledger).
			a.logger.WarnContext(ctx, "billing: MO suppressed — meter at floor, not accrued",
				"customer_id", owner.CustomerID, "owner_type", owner.Type, "owner_id", owner.ID, "message_id", messageID)
			return MOResult{Balance: toInt(res[1]), Suppressed: true, Charged: 0}, nil

		case "charged":
			newBalance, crossed := toInt(res[1]), toInt(res[2]) == 1
			_, applied, err := a.store.RecordDurable(ctx, a.moEntry(owner, messageID, -credits))
			if err != nil {
				// Undo the speculative cache debit on an un-cancellable context, then fail closed. INCRBY
				// preserves the key's TTL; DEL of the seen-key lets a legitimate retry re-accrue.
				a.undoMOCacheDebit(ctx, bkey, skey, credits, messageID)
				return MOResult{}, fmt.Errorf("billing: record MO durable: %w", err)
			}
			if !applied {
				// A replay past the seen-TTL: the durable meter already holds it. Undo the speculative
				// cache re-debit so the meter cannot drift below the durable value.
				a.undoMOCacheDebit(ctx, bkey, skey, credits, messageID)
				return MOResult{Balance: newBalance + credits, Charged: 0}, nil
			}
			if crossed {
				a.logger.WarnContext(ctx, "billing: MO meter reached its floor",
					"customer_id", owner.CustomerID, "owner_type", owner.Type, "owner_id", owner.ID,
					"balance", newBalance, "floor", floor)
			}
			return MOResult{Balance: newBalance, Charged: credits, FloorReached: crossed}, nil

		default:
			return MOResult{}, fmt.Errorf("billing: record MO unexpected status %q", status)
		}
	}
	return MOResult{}, fmt.Errorf("billing: record MO %s: still cold after rehydration", messageID)
}

// undoMOCacheDebit reverses a speculative recordmo.lua meter debit whose durable side did not stick (a
// replay past the seen-TTL, or a durable failure). undomo.lua adds the credits back only if the meter key
// still exists (preserving its TTL) — never resurrecting an expired key as a phantom positive value — and
// clears the seen-key so a legitimate retry can re-accrue. On an un-cancellable context so a timed-out
// request still compensates.
func (a *Accountant) undoMOCacheDebit(ctx context.Context, bkey, skey string, credits int, messageID uuid.UUID) {
	ctx = context.WithoutCancel(ctx)
	if err := a.undoMO.Run(ctx, a.rdb, []string{bkey, skey}, credits).Err(); err != nil {
		a.logger.ErrorContext(ctx, "billing: MO cache undo failed — meter may drift until rehydration",
			"message_id", messageID, "err", err)
	}
}

// undoReserveCacheDebit reverses a speculative reserve.lua cache debit whose durable side did not stick —
// a cross-partition replay (RecordDurable applied=false) or a genuine durable failure. release.lua refunds
// the fresh hold in place if it is still live ("released"); if a concurrent actor already consumed the
// hold (e.g. a duplicate capture), the debit cannot be refunded in place, so the cache is DROPPED and the
// next reserve rehydrates from the durable authority. Any brief cache/durable divergence this leaves is
// bounded by the balance cache's TTL (step-142b); the durable ledger is never touched here.
func (a *Accountant) undoReserveCacheDebit(ctx context.Context, bkey, rkey string, messageID uuid.UUID) {
	ctx = context.WithoutCancel(ctx) // a cancelled/timed-out request must still undo the cache debit
	res, err := a.release.Run(ctx, a.rdb, []string{bkey, rkey}).Slice()
	if err != nil {
		a.logger.ErrorContext(ctx, "billing: reserve cache undo failed — cache may drift until rehydration",
			"message_id", messageID, "err", err)
		return
	}
	if status, _ := res[0].(string); status != "released" {
		// The fresh hold was gone (consumed concurrently, or the cache had lapsed): the debit could not be
		// refunded in place. Drop the cache so it rehydrates from the durable authority, not a stuck value.
		if delErr := a.rdb.Del(ctx, bkey).Err(); delErr != nil {
			a.logger.WarnContext(ctx, "billing: reserve cache undo could not clear the balance cache",
				"message_id", messageID, "err", delErr)
		}
	}
}

// rehydrate loads the durable balance into the cold cache with SET NX, so a concurrent rehydration cannot
// clobber a fresher value. A durable-read failure is fatal (fail-closed): a credit is never passed
// unverified.
func (a *Accountant) rehydrate(ctx context.Context, bkey string, owner Owner) error {
	bal, found, err := a.store.Balance(ctx, owner.Type, owner.ID, directionMT)
	if err != nil {
		return fmt.Errorf("billing: rehydrate balance (fail-closed): %w", err)
	}
	if !found {
		bal = 0
	}
	// A BOUNDED TTL (not 0): the cache expires balanceTTL after this rehydrate (reserve.lua/release.lua
	// preserve it with KEEPTTL), so any cache/durable drift self-heals on the next rehydrate. SetNX still
	// guards against clobbering a concurrently-warmed value.
	if err := a.rdb.SetNX(ctx, bkey, bal, a.balanceTTL).Err(); err != nil {
		return fmt.Errorf("billing: rehydrate set: %w", err)
	}
	return nil
}

func (a *Accountant) entry(owner Owner, messageID *uuid.UUID, et cp.EntryType, credits int) cp.LedgerEntry {
	return cp.LedgerEntry{
		OwnerType: owner.Type, OwnerID: owner.ID, Direction: directionMT,
		CustomerID: owner.CustomerID, AccountID: owner.AccountID, MessageID: messageID,
		EntryType: et, Credits: credits,
	}
}

// moEntry builds an MO meter ledger entry (direction=mo, entry_type=mo_charge, credits<0).
func (a *Accountant) moEntry(owner Owner, messageID uuid.UUID, credits int) cp.LedgerEntry {
	return cp.LedgerEntry{
		OwnerType: owner.Type, OwnerID: owner.ID, Direction: directionMO,
		CustomerID: owner.CustomerID, AccountID: owner.AccountID, MessageID: &messageID,
		EntryType: cp.EntryMOCharge, Credits: credits,
	}
}

// rehydrateMO loads the durable MO meter into the cold cache with SET NX + bounded TTL, mirroring
// rehydrate for the MO direction. Fail-closed on a durable-read error.
func (a *Accountant) rehydrateMO(ctx context.Context, bkey string, owner Owner) error {
	bal, found, err := a.store.Balance(ctx, owner.Type, owner.ID, directionMO)
	if err != nil {
		return fmt.Errorf("billing: rehydrate MO balance (fail-closed): %w", err)
	}
	if !found {
		bal = 0
	}
	if err := a.rdb.SetNX(ctx, bkey, bal, a.balanceTTL).Err(); err != nil {
		return fmt.Errorf("billing: rehydrate MO set: %w", err)
	}
	return nil
}

// terminalLockTTL bounds a crashed holder's lock; terminalCriticalTimeout bounds the critical section
// BELOW it, so a live-but-slow holder always finishes and releases before the lock could expire under it
// (which would admit a second, racing terminal). terminalLockWait spins generously past the TTL so a
// waiter always eventually acquires — a live holder releases fast, a crashed holder's lock self-expires.
const (
	terminalLockTTL         = 5 * time.Second
	terminalCriticalTimeout = 4 * time.Second
	terminalLockWait        = 2 * terminalLockTTL
)

// withTerminalLock serialises the durable resolution of a message's TERMINAL state (capture vs release):
// capture and release are DIFFERENT entry_types, so the DB unique index cannot stop both from writing —
// only this lock, plus the in-fn opposite-terminal check, gives mutual exclusion. It NEVER runs fn without
// the lock: two opposite outcomes racing unlocked could both pass their checks and both write (one message
// captured AND released → the delivered charge silently refunded). The waiter blocks, then its re-check
// sees the winner and yields (first durable entry wins). Only the rare terminal paths take it, never the
// hot reserve path.
func (a *Accountant) withTerminalLock(ctx context.Context, messageID uuid.UUID, fn func(context.Context) error) error {
	lockKey := "billing:terminal:" + messageID.String()
	for waited := time.Duration(0); waited < terminalLockWait; waited += 20 * time.Millisecond {
		ok, err := a.rdb.SetNX(ctx, lockKey, "1", terminalLockTTL).Result()
		if err != nil {
			return fmt.Errorf("billing: terminal lock: %w", err)
		}
		if ok {
			defer func() { _ = a.rdb.Del(context.WithoutCancel(ctx), lockKey).Err() }()
			// Bound the critical section below the lock TTL so a slow Postgres can never let the lock lapse
			// while we still hold it (which would admit a second, racing terminal).
			cctx, cancel := context.WithTimeout(ctx, terminalCriticalTimeout)
			defer cancel()
			return fn(cctx)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	// A holder stuck past both its critical-section bound and its lock TTL — fail closed rather than run
	// unlocked. The terminal op is retryable (the caller re-drives it; the reserve hold/ledger are intact).
	return fmt.Errorf("billing: terminal lock %s not acquired within %s (stuck holder): %w",
		messageID, terminalLockWait, errs.ErrConflict)
}

// toInt reads a Lua-returned integer (go-redis decodes RESP integers as int64).
func toInt(v any) int {
	if n, ok := v.(int64); ok {
		return int(n)
	}
	return 0
}
