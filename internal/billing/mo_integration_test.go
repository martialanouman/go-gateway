package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestRecordMOAccruesAndLedger proves the MO meter accrues a negative balance, writes a mo_charge ledger
// entry per message (direction=mo), rehydrates the cold cache from Postgres, and keeps SUM(credits)==meter.
func TestRecordMOAccruesAndLedger(t *testing.T) {
	h := newBillingHarness(t, 100) // 100 is the MT balance; the MO meter starts at 0 (cold)
	ctx := context.Background()

	msg1 := uuid.New()
	r1, err := h.acc.RecordMO(ctx, h.owner, msg1, 4) // cold → rehydrate from durable 0 → -4
	if err != nil {
		t.Fatalf("RecordMO 1: %v", err)
	}
	if r1.Balance != -4 || r1.Charged != 4 || r1.Suppressed || r1.FloorReached {
		t.Errorf("RecordMO 1 = %+v, want {Balance:-4 Charged:4}", r1)
	}
	if h.moBalance(t) != -4 {
		t.Errorf("durable MO meter = %d, want -4", h.moBalance(t))
	}

	r2, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 6) // -4 → -10
	if err != nil {
		t.Fatalf("RecordMO 2: %v", err)
	}
	if r2.Balance != -10 || r2.Charged != 6 {
		t.Errorf("RecordMO 2 = %+v, want {Balance:-10 Charged:6}", r2)
	}

	// A mo_charge ledger row exists for msg1 with direction=mo and credits=-4.
	var credits int
	var direction string
	if err := pgtest.Pool(t).QueryRow(ctx,
		`SELECT credits, direction FROM control_plane.billing_ledger WHERE message_id = $1 AND entry_type = 'mo_charge'`,
		msg1).Scan(&credits, &direction); err != nil {
		t.Fatalf("read mo_charge ledger: %v", err)
	}
	if credits != -4 || direction != cp.BillingDirectionMO {
		t.Errorf("ledger mo_charge = (credits %d, dir %q), want (-4, mo)", credits, direction)
	}
	// The MT balance is untouched by MO accrual.
	if h.balance(t) != 100 {
		t.Errorf("MT balance = %d, want 100 (MO must not touch MT)", h.balance(t))
	}
}

// TestRecordMOFloorStopsAndAlertsOnce proves full-then-stop: the meter accrues in full until one MO crosses
// mo_billing_floor (FloorReached once), after which further MOs are Suppressed and never accrued.
func TestRecordMOFloorStopsAndAlertsOnce(t *testing.T) {
	h := newBillingHarness(t, 100)
	floor := -10
	h.setConfig(cp.BillingCustomer{BillingMode: cp.BillingPostpaid, MoBillingFloor: &floor})
	ctx := context.Background()

	// 0 → -4 → -8: below-floor not yet reached, no alert.
	for i := 0; i < 2; i++ {
		r, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 4)
		if err != nil {
			t.Fatalf("RecordMO %d: %v", i, err)
		}
		if r.FloorReached || r.Suppressed {
			t.Errorf("RecordMO %d = %+v, want accruing (no floor/suppress)", i, r)
		}
	}
	if h.moBalance(t) != -8 {
		t.Fatalf("meter = %d, want -8", h.moBalance(t))
	}

	// -8 → -12: crosses the -10 floor, accrued in full, alert exactly once.
	rc, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 4)
	if err != nil {
		t.Fatalf("RecordMO crossing: %v", err)
	}
	if !rc.FloorReached || rc.Charged != 4 || rc.Balance != -12 {
		t.Errorf("crossing = %+v, want {Balance:-12 Charged:4 FloorReached:true}", rc)
	}

	// Now at/below floor: further MOs are suppressed, not accrued, no alert, meter unchanged.
	rs, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 4)
	if err != nil {
		t.Fatalf("RecordMO suppressed: %v", err)
	}
	if !rs.Suppressed || rs.Charged != 0 || rs.FloorReached {
		t.Errorf("suppressed = %+v, want {Suppressed:true Charged:0}", rs)
	}
	if h.moBalance(t) != -12 {
		t.Errorf("meter after suppressed = %d, want -12 (accrual stopped)", h.moBalance(t))
	}
}

// TestRecordMONeverBlocksMT is the cross-axis guard: a meter driven to its floor does not affect the MT
// balance — an MT reserve/capture still succeeds while the MO meter is stopped.
func TestRecordMONeverBlocksMT(t *testing.T) {
	h := newBillingHarness(t, 50)
	floor := -5
	h.setConfig(cp.BillingCustomer{BillingMode: cp.BillingPostpaid, MoBillingFloor: &floor})
	ctx := context.Background()

	// 0 → -3 (accrues), -3 → -6 crosses the -5 floor, then the next is suppressed.
	if r, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 3); err != nil || r.Charged != 3 {
		t.Fatalf("RecordMO 1 = (%+v, %v), want Charged 3", r, err)
	}
	if r, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 3); err != nil || !r.FloorReached {
		t.Fatalf("RecordMO crossing = (%+v, %v), want FloorReached", r, err)
	}
	if r, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 3); err != nil || !r.Suppressed {
		t.Fatalf("RecordMO suppressed = (%+v, %v), want Suppressed", r, err)
	}

	// The MT balance is fully usable despite the MO meter being at its floor.
	msg := uuid.New()
	if _, err := h.acc.Reserve(ctx, h.owner, msg, 20); err != nil {
		t.Fatalf("MT Reserve while MO floored: %v", err)
	}
	if _, err := h.acc.Capture(ctx, h.owner, msg); err != nil {
		t.Fatalf("MT Capture while MO floored: %v", err)
	}
	if h.balance(t) != 30 {
		t.Errorf("MT balance = %d, want 30 (MO floor must not affect MT)", h.balance(t))
	}
}

// TestRecordMOIdempotentReplay proves a redelivered MO (same message_id) accrues exactly once — the
// seen-key catches the replay before it touches the meter.
func TestRecordMOIdempotentReplay(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()
	msg := uuid.New()

	r1, err := h.acc.RecordMO(ctx, h.owner, msg, 5)
	if err != nil || r1.Charged != 5 {
		t.Fatalf("RecordMO 1 = (%+v, %v), want Charged 5", r1, err)
	}
	r2, err := h.acc.RecordMO(ctx, h.owner, msg, 5) // replay
	if err != nil {
		t.Fatalf("RecordMO replay: %v", err)
	}
	if r2.Charged != 0 || r2.Balance != -5 {
		t.Errorf("replay = %+v, want {Charged:0 Balance:-5} (accrued once)", r2)
	}
	if h.moBalance(t) != -5 {
		t.Errorf("durable meter = %d, want -5 (idempotent)", h.moBalance(t))
	}
	// Exactly one mo_charge ledger row for the message.
	var n int
	if err := pgtest.Pool(t).QueryRow(ctx,
		`SELECT count(*) FROM control_plane.billing_ledger WHERE message_id = $1 AND entry_type = 'mo_charge'`,
		msg).Scan(&n); err != nil {
		t.Fatalf("count mo_charge: %v", err)
	}
	if n != 1 {
		t.Errorf("mo_charge rows = %d, want 1", n)
	}
}

// TestRecordMOFloorFromRepo exercises the PRODUCTION config path (config-sync → ListBillingCustomers →
// BuildConfigSnapshot → MOFloor), not the hand-built test snapshot: a mo_billing_floor set in Postgres must
// reach the meter and stop accrual. Guards against the floor being dropped in the repo→snapshot mapping.
func TestRecordMOFloorFromRepo(t *testing.T) {
	h := newBillingHarness(t, 100)
	ctx := context.Background()

	if _, err := pgtest.Pool(t).Exec(ctx,
		`UPDATE control_plane.customers SET billing_enabled = true, billing_mode = 'postpaid', mo_billing_floor = -6 WHERE id = $1`,
		h.owner.CustomerID); err != nil {
		t.Fatalf("configure mo_billing_floor: %v", err)
	}
	snap, err := billing.LoadConfigSnapshot(ctx, h.repo)
	if err != nil {
		t.Fatalf("LoadConfigSnapshot: %v", err)
	}
	h.cfg.Store(snap)

	// 0 → -4 accrues; -4 → -8 crosses the -6 floor loaded from Postgres; the next is suppressed.
	if r, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 4); err != nil || r.Charged != 4 || r.FloorReached {
		t.Fatalf("RecordMO 1 = (%+v, %v), want accruing", r, err)
	}
	if r, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 4); err != nil || !r.FloorReached {
		t.Fatalf("RecordMO crossing = (%+v, %v), want FloorReached (floor from repo)", r, err)
	}
	if r, err := h.acc.RecordMO(ctx, h.owner, uuid.New(), 4); err != nil || !r.Suppressed {
		t.Fatalf("RecordMO suppressed = (%+v, %v), want Suppressed", r, err)
	}
}

// TestRecordMOReplayPastSeenTTL proves the durable idempotency layer: once the seen-key has expired, a
// redelivery is caught durably (RecordDurable applied=false) and the speculative cache debit is undone —
// the meter still accrues exactly once.
func TestRecordMOReplayPastSeenTTL(t *testing.T) {
	h := newBillingHarnessTTL(t, 100, time.Minute, billing.WithMOSeenTTL(200*time.Millisecond))
	ctx := context.Background()
	msg := uuid.New()

	if r, err := h.acc.RecordMO(ctx, h.owner, msg, 7); err != nil || r.Charged != 7 {
		t.Fatalf("RecordMO 1 = (%+v, %v), want Charged 7", r, err)
	}
	time.Sleep(300 * time.Millisecond) // let the seen-key lapse

	r2, err := h.acc.RecordMO(ctx, h.owner, msg, 7) // replay past the seen window
	if err != nil {
		t.Fatalf("RecordMO replay past TTL: %v", err)
	}
	if r2.Charged != 0 {
		t.Errorf("replay past TTL = %+v, want Charged 0 (durable idempotency)", r2)
	}
	if h.moBalance(t) != -7 {
		t.Errorf("durable meter = %d, want -7 (accrued once despite seen-key expiry)", h.moBalance(t))
	}
}
