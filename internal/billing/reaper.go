package billing

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// defaultReaperMinAge is how long a reservation must sit unsettled before the reaper touches it. It is
// deliberately far above a message's normal time to a terminal outcome: the nominal settle happens
// milliseconds after the SMSC responds, so anything still open after this window is genuinely stuck, not
// in flight. Too short and the reaper races connector-pool and settles live messages.
const defaultReaperMinAge = 15 * time.Minute

// defaultReaperBatch bounds one pass. The sweep is periodic, so a backlog drains over several passes
// rather than in one unbounded query — the only read in this service able to stall it.
const defaultReaperBatch = 500

// OrphanSource yields reservations the settle loop never closed. *postgres.BillingRepo satisfies it;
// declared consumer-side (convention §2).
type OrphanSource interface {
	OrphanedReservations(ctx context.Context, olderThan time.Time, limit int) ([]cp.OrphanedReservation, error)
}

// OutcomeReader resolves what actually happened to a message. *clickhouse.CDRReader satisfies it. It
// returns the CURRENT status: the CDR is a ReplacingMergeTree, so the implementation must collapse to the
// highest version — reading a stale row would hand the reaper the initial `accepted` of a message long
// since delivered. found=false means no CDR row exists for the message at all.
type OutcomeReader interface {
	MessageStatus(ctx context.Context, messageID uuid.UUID) (status string, found bool, err error)
}

// ReaperSettler closes the money. *Accountant satisfies it: capture and release are idempotent and
// mutually exclusive per message_id (resolveTerminal), so the reaper can never double-settle a message
// the nominal path settled concurrently, nor apply both outcomes.
type ReaperSettler interface {
	Capture(ctx context.Context, owner Owner, messageID uuid.UUID) (creditsCharged int, err error)
	Release(ctx context.Context, owner Owner, messageID uuid.UUID) error
}

// ReaperMetric counts what a pass did. Reaped is labelled by action (a bounded label — never a message id
// or customer id). Unresolvable counts reservations whose outcome could not be established: those are left
// intact, so a rising count is an audit gap an operator must investigate, and it MUST alert.
type ReaperMetric interface {
	Reaped(action string)
	Unresolvable()
}

type nopReaperMetric struct{}

func (nopReaperMetric) Reaped(string) {}
func (nopReaperMetric) Unresolvable() {}

// reaperAction is the decision for one orphaned reservation.
type reaperAction int

const (
	// actionNone covers both the transient states and the unknown ones: do nothing, keep the money as it
	// stands, revisit next pass. It is the safe default in every ambiguous case.
	actionNone reaperAction = iota
	actionCapture
	actionRelease
)

func (a reaperAction) String() string {
	switch a {
	case actionCapture:
		return "capture"
	case actionRelease:
		return "release"
	default:
		return "none"
	}
}

// decideFromStatus maps a CDR lifecycle status to a settlement. The statuses fall in THREE camps, not
// two, and conflating the third with either of the others loses money:
//
//   - enroute, delivered → capture. The message reached the SMSC (ESME_ROK); the delivery is owed.
//   - failed, expired, rejected, cancelled → release. It never left; the customer must be refunded.
//   - accepted, rerouted → nothing. `accepted` means ingested but not yet submitted, and `rerouted` is a
//     transit step towards a fallback connector. Both are still in flight: capturing would charge for an
//     unsent message, releasing would refund one about to be sent.
//
// An unrecognised status also yields actionNone: a status this code does not know is not a licence to
// guess with somebody's money.
func decideFromStatus(status string) reaperAction {
	switch status {
	case "enroute", "delivered":
		return actionCapture
	case "failed", "expired", "rejected", "cancelled":
		return actionRelease
	default:
		return actionNone
	}
}

// Reaper reconciles reservations the MT settle loop left open (step-190). connector-pool settles
// FAIL-OPEN — a billing fault there is logged and swallowed, never returned, because propagating it would
// redeliver the record and re-send the SMS — which means a billing outage leaves reserve debits standing
// with nothing to close them: the customer stays charged for a message that may never have been sent.
// The reaper is that missing net.
//
// Detection is driven by the LEDGER (the money authority) and the decision by the CDR (the only record of
// what actually happened). It NEVER releases blind: an outcome it cannot establish leaves the reservation
// untouched and raises an alert, because refunding a message that was really sent is a free delivery.
type Reaper struct {
	source   OrphanSource
	outcomes OutcomeReader
	settler  ReaperSettler
	minAge   time.Duration
	batch    int
	metric   ReaperMetric
	logger   *slog.Logger
}

// ReaperOption configures a Reaper.
type ReaperOption func(*Reaper)

// WithMinAge overrides how long a reservation must be unsettled before it is swept (default 15min).
func WithMinAge(d time.Duration) ReaperOption {
	return func(r *Reaper) {
		if d > 0 {
			r.minAge = d
		}
	}
}

// WithReaperBatch overrides the per-pass row cap (default 500).
func WithReaperBatch(n int) ReaperOption {
	return func(r *Reaper) {
		if n > 0 {
			r.batch = n
		}
	}
}

// WithReaperMetric wires the pass counters.
func WithReaperMetric(m ReaperMetric) ReaperOption {
	return func(r *Reaper) {
		if m != nil {
			r.metric = m
		}
	}
}

// WithReaperLogger sets the logger (defaults to slog.Default).
func WithReaperLogger(l *slog.Logger) ReaperOption {
	return func(r *Reaper) {
		if l != nil {
			r.logger = l
		}
	}
}

// NewReaper builds the reaper over its detection source, outcome store and settler.
func NewReaper(source OrphanSource, outcomes OutcomeReader, settler ReaperSettler, opts ...ReaperOption) *Reaper {
	r := &Reaper{
		source: source, outcomes: outcomes, settler: settler,
		minAge: defaultReaperMinAge, batch: defaultReaperBatch,
		metric: nopReaperMetric{}, logger: slog.Default(),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// ReapOnce runs one reconciliation pass. A detection failure aborts and is RETURNED so the supervised
// ticker logs it and retries — a swallowed failure would look like a healthy empty pass. A per-reservation
// failure is logged and skipped: one stuck message must never block every other customer's money.
func (r *Reaper) ReapOnce(ctx context.Context) error {
	orphans, err := r.source.OrphanedReservations(ctx, time.Now().Add(-r.minAge), r.batch)
	if err != nil {
		return err
	}
	for _, o := range orphans {
		r.settleOne(ctx, o)
	}
	return nil
}

// settleOne resolves and settles a single orphaned reservation. Every path that cannot establish the
// outcome with certainty leaves the money exactly as it is.
func (r *Reaper) settleOne(ctx context.Context, o cp.OrphanedReservation) {
	status, found, err := r.outcomes.MessageStatus(ctx, o.MessageID)
	if err != nil {
		// The outcome store is unreachable. Do NOT guess: skip and let a later pass resolve it.
		r.logger.WarnContext(ctx, "reaper: outcome read failed; leaving reservation intact",
			"message_id", o.MessageID, "err", err)
		return
	}
	if !found {
		// A reserve with no CDR row at all is an audit gap, not a licence to refund.
		r.metric.Unresolvable()
		r.logger.WarnContext(ctx, "reaper: reservation has no CDR outcome; left intact for an operator",
			"message_id", o.MessageID, "customer_id", o.CustomerID, "credits", o.Credits,
			"reserved_at", o.ReservedAt)
		return
	}

	action := decideFromStatus(status)
	if action == actionNone {
		// Still in flight (accepted/rerouted) or a status this code does not model. Revisit next pass.
		return
	}

	owner := ownerFromReservation(o)
	switch action {
	case actionCapture:
		if _, err := r.settler.Capture(ctx, owner, o.MessageID); err != nil {
			r.logger.WarnContext(ctx, "reaper: capture failed; will retry next pass",
				"message_id", o.MessageID, "status", status, "err", err)
			return
		}
	case actionRelease:
		if err := r.settler.Release(ctx, owner, o.MessageID); err != nil {
			r.logger.WarnContext(ctx, "reaper: release failed; will retry next pass",
				"message_id", o.MessageID, "status", status, "err", err)
			return
		}
	case actionNone:
		return
	}
	r.metric.Reaped(action.String())
	r.logger.InfoContext(ctx, "reaper: reconciled an orphaned reservation",
		"message_id", o.MessageID, "status", status, "action", action.String(),
		"customer_id", o.CustomerID, "credits", o.Credits, "reserved_at", o.ReservedAt)
}

// ownerFromReservation rebuilds the balance owner the reserve debited, so the settlement hits the
// IDENTICAL key (step-145). Settling an smpp_account-scoped reservation against the customer key would
// move money on the wrong balance and leave both wrong.
func ownerFromReservation(o cp.OrphanedReservation) Owner {
	return Owner{
		Type:       o.OwnerType,
		ID:         o.OwnerID,
		CustomerID: o.CustomerID,
		AccountID:  o.AccountID,
	}
}
