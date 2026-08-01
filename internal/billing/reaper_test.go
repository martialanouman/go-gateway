package billing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// --- fakes -------------------------------------------------------------------------------------------

type fakeOrphanSource struct {
	rows []cp.OrphanedReservation
	err  error
	// cutoff records the age threshold the reaper asked for, so a test can prove it is applied.
	cutoff time.Time
}

func (f *fakeOrphanSource) OrphanedReservations(_ context.Context, olderThan time.Time, _ int) ([]cp.OrphanedReservation, error) {
	f.cutoff = olderThan
	return f.rows, f.err
}

type fakeOutcomeReader struct {
	status map[uuid.UUID]string
	err    error
}

func (f *fakeOutcomeReader) MessageStatus(_ context.Context, messageID uuid.UUID) (string, bool, error) {
	if f.err != nil {
		return "", false, f.err
	}
	s, ok := f.status[messageID]
	return s, ok, nil
}

type recordedSettle struct {
	messageID uuid.UUID
	owner     billing.Owner
}

type fakeSettler struct {
	captured []recordedSettle
	released []recordedSettle
	err      error
}

func (f *fakeSettler) Capture(_ context.Context, owner billing.Owner, messageID uuid.UUID) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.captured = append(f.captured, recordedSettle{messageID, owner})
	return 3, nil
}

func (f *fakeSettler) Release(_ context.Context, owner billing.Owner, messageID uuid.UUID) error {
	if f.err != nil {
		return f.err
	}
	f.released = append(f.released, recordedSettle{messageID, owner})
	return nil
}

type countingMetric struct{ captured, released, unresolvable int }

func (c *countingMetric) Reaped(action string) {
	switch action {
	case "capture":
		c.captured++
	case "release":
		c.released++
	}
}
func (c *countingMetric) Unresolvable() { c.unresolvable++ }

func orphan(messageID uuid.UUID) cp.OrphanedReservation {
	customerID := uuid.New()
	return cp.OrphanedReservation{
		MessageID: messageID, OwnerType: cp.OwnerTypeCustomer, OwnerID: customerID,
		CustomerID: customerID, Credits: -3, ReservedAt: time.Now().Add(-time.Hour),
	}
}

// --- tests -------------------------------------------------------------------------------------------

// TestReaperSettlesByOutcome proves the core decision table: the CDR outcome — not the age, not a default
// — decides whether an orphaned reservation is captured or released. A message that reached the SMSC
// (enroute, and its later delivered) is CAPTURED: the delivery happened and the customer owes for it. A
// message that never left (failed/expired/rejected/cancelled) is RELEASED and the customer refunded.
func TestReaperSettlesByOutcome(t *testing.T) {
	capture := []string{"enroute", "delivered"}
	release := []string{"failed", "expired", "rejected", "cancelled"}

	for _, status := range capture {
		t.Run("capture/"+status, func(t *testing.T) {
			id := uuid.New()
			src := &fakeOrphanSource{rows: []cp.OrphanedReservation{orphan(id)}}
			out := &fakeOutcomeReader{status: map[uuid.UUID]string{id: status}}
			settler := &fakeSettler{}
			metric := &countingMetric{}

			r := billing.NewReaper(src, out, settler, billing.WithReaperMetric(metric))
			if err := r.ReapOnce(context.Background()); err != nil {
				t.Fatalf("ReapOnce: %v", err)
			}
			if len(settler.captured) != 1 || settler.captured[0].messageID != id {
				t.Fatalf("status %q: captured %+v, want exactly the orphan %s", status, settler.captured, id)
			}
			if len(settler.released) != 0 {
				t.Errorf("status %q: released %+v — a sent message refunded is a free delivery", status, settler.released)
			}
			if metric.captured != 1 {
				t.Errorf("status %q: captured metric = %d, want 1", status, metric.captured)
			}
		})
	}

	for _, status := range release {
		t.Run("release/"+status, func(t *testing.T) {
			id := uuid.New()
			src := &fakeOrphanSource{rows: []cp.OrphanedReservation{orphan(id)}}
			out := &fakeOutcomeReader{status: map[uuid.UUID]string{id: status}}
			settler := &fakeSettler{}
			metric := &countingMetric{}

			r := billing.NewReaper(src, out, settler, billing.WithReaperMetric(metric))
			if err := r.ReapOnce(context.Background()); err != nil {
				t.Fatalf("ReapOnce: %v", err)
			}
			if len(settler.released) != 1 || settler.released[0].messageID != id {
				t.Fatalf("status %q: released %+v, want exactly the orphan %s", status, settler.released, id)
			}
			if len(settler.captured) != 0 {
				t.Errorf("status %q: captured %+v — charging for a message that never left", status, settler.captured)
			}
			if metric.released != 1 {
				t.Errorf("status %q: released metric = %d, want 1", status, metric.released)
			}
		})
	}
}

// TestReaperLeavesTransientOutcomesAlone proves the third camp. accepted ("ingested, not yet submitted")
// and rerouted (in transit to a fallback connector) are NOT terminal: settling them would either charge
// for a message still in flight or refund one about to be sent. They must survive to the next pass.
func TestReaperLeavesTransientOutcomesAlone(t *testing.T) {
	for _, status := range []string{"accepted", "rerouted"} {
		t.Run(status, func(t *testing.T) {
			id := uuid.New()
			src := &fakeOrphanSource{rows: []cp.OrphanedReservation{orphan(id)}}
			out := &fakeOutcomeReader{status: map[uuid.UUID]string{id: status}}
			settler := &fakeSettler{}
			metric := &countingMetric{}

			r := billing.NewReaper(src, out, settler, billing.WithReaperMetric(metric))
			if err := r.ReapOnce(context.Background()); err != nil {
				t.Fatalf("ReapOnce: %v", err)
			}
			if len(settler.captured) != 0 || len(settler.released) != 0 {
				t.Fatalf("status %q settled (captured %d, released %d) — the message is still in flight",
					status, len(settler.captured), len(settler.released))
			}
			// Transient is not an anomaly: it must not raise the alert metric either.
			if metric.unresolvable != 0 {
				t.Errorf("status %q raised the unresolvable alert %d time(s); it is a normal in-flight state",
					status, metric.unresolvable)
			}
		})
	}
}

// TestReaperNeverReleasesBlind is the money-safety guard. When the outcome cannot be established — no CDR
// row at all, or the CDR read failed — the reservation is LEFT INTACT and counted on the alert metric.
// Releasing by default would refund messages that were really sent: a free delivery, i.e. revenue loss.
func TestReaperNeverReleasesBlind(t *testing.T) {
	t.Run("no CDR row", func(t *testing.T) {
		id := uuid.New()
		src := &fakeOrphanSource{rows: []cp.OrphanedReservation{orphan(id)}}
		out := &fakeOutcomeReader{status: map[uuid.UUID]string{}} // unknown message
		settler := &fakeSettler{}
		metric := &countingMetric{}

		r := billing.NewReaper(src, out, settler, billing.WithReaperMetric(metric))
		if err := r.ReapOnce(context.Background()); err != nil {
			t.Fatalf("ReapOnce: %v", err)
		}
		if len(settler.released) != 0 {
			t.Fatal("released a reservation with no known outcome — a really-sent message would be refunded for free")
		}
		if len(settler.captured) != 0 {
			t.Fatal("captured a reservation with no known outcome — charging on a guess")
		}
		if metric.unresolvable != 1 {
			t.Errorf("unresolvable metric = %d, want 1 (this must alert an operator)", metric.unresolvable)
		}
	})

	t.Run("CDR read fails", func(t *testing.T) {
		id := uuid.New()
		src := &fakeOrphanSource{rows: []cp.OrphanedReservation{orphan(id)}}
		out := &fakeOutcomeReader{err: errors.New("clickhouse down")}
		settler := &fakeSettler{}

		r := billing.NewReaper(src, out, settler)
		if err := r.ReapOnce(context.Background()); err != nil {
			t.Fatalf("ReapOnce: %v", err)
		}
		if len(settler.released) != 0 || len(settler.captured) != 0 {
			t.Fatal("settled while the outcome store was unreachable — the reaper must not guess")
		}
	})
}

// TestReaperSettlesAgainstTheReservedOwner proves the owner is rebuilt from what the reservation pinned,
// so the settlement hits the identical balance key the reserve debited. An smpp_account-scoped customer
// settled against the customer key would move money on the wrong balance and leave both wrong.
func TestReaperSettlesAgainstTheReservedOwner(t *testing.T) {
	id, accountID, customerID := uuid.New(), uuid.New(), uuid.New()
	src := &fakeOrphanSource{rows: []cp.OrphanedReservation{{
		MessageID: id, OwnerType: cp.OwnerTypeSMPPAccount, OwnerID: accountID,
		CustomerID: customerID, AccountID: &accountID, Credits: -2, ReservedAt: time.Now().Add(-time.Hour),
	}}}
	out := &fakeOutcomeReader{status: map[uuid.UUID]string{id: "delivered"}}
	settler := &fakeSettler{}

	r := billing.NewReaper(src, out, settler)
	if err := r.ReapOnce(context.Background()); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	if len(settler.captured) != 1 {
		t.Fatalf("captured %d, want 1", len(settler.captured))
	}
	got := settler.captured[0].owner
	if got.Type != cp.OwnerTypeSMPPAccount || got.ID != accountID || got.CustomerID != customerID {
		t.Errorf("owner = %+v, want smpp_account %s of customer %s", got, accountID, customerID)
	}
	if got.AccountID == nil || *got.AccountID != accountID {
		t.Errorf("AccountID = %v, want %s (the ledger attributes the charge to the originating account)", got.AccountID, accountID)
	}
}

// TestReaperOneFailureDoesNotAbortThePass proves a pass is resilient: a settle error on one reservation is
// logged and skipped, the others still get reconciled. A pass that aborted on the first bad row would let
// one stuck message block every other customer's money indefinitely.
func TestReaperOneFailureDoesNotAbortThePass(t *testing.T) {
	bad, good := uuid.New(), uuid.New()
	src := &fakeOrphanSource{rows: []cp.OrphanedReservation{orphan(bad), orphan(good)}}
	out := &fakeOutcomeReader{status: map[uuid.UUID]string{good: "delivered"}} // `bad` has no outcome
	settler := &fakeSettler{}

	r := billing.NewReaper(src, out, settler)
	if err := r.ReapOnce(context.Background()); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	if len(settler.captured) != 1 || settler.captured[0].messageID != good {
		t.Errorf("captured %+v, want the resolvable orphan %s to still be settled", settler.captured, good)
	}
}

// TestReaperAppliesTheAgeCutoff proves the reaper asks for reservations older than its configured minimum
// age, rather than sweeping everything. Without it the reaper races the connector pool's nominal settle.
func TestReaperAppliesTheAgeCutoff(t *testing.T) {
	src := &fakeOrphanSource{}
	r := billing.NewReaper(src, &fakeOutcomeReader{}, &fakeSettler{}, billing.WithMinAge(30*time.Minute))

	before := time.Now()
	if err := r.ReapOnce(context.Background()); err != nil {
		t.Fatalf("ReapOnce: %v", err)
	}
	// The cutoff must sit ~30min in the past, never in the future.
	if src.cutoff.After(before.Add(-29 * time.Minute)) {
		t.Errorf("cutoff = %v, want roughly 30min before %v", src.cutoff, before)
	}
}

// TestReaperSourceFailureIsReturned proves a detection-query failure aborts the pass and surfaces, so the
// supervised ticker can log it and retry — as opposed to a silent no-op pass that would look healthy.
func TestReaperSourceFailureIsReturned(t *testing.T) {
	src := &fakeOrphanSource{err: errors.New("postgres down")}
	r := billing.NewReaper(src, &fakeOutcomeReader{}, &fakeSettler{})
	if err := r.ReapOnce(context.Background()); err == nil {
		t.Fatal("ReapOnce = nil, want the detection failure surfaced")
	}
}
