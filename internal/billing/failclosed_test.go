package billing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// failingStore is a LedgerStore whose durable read fails. RecordDurable records that it was reached, so a
// test can prove a denied reserve never mirrored anything.
//
// It used to stand in for an unreachable Postgres; since step-260b it does not, and should not be read that
// way. A real outage travels through postgres.translate, which wraps an unrecognised pgx failure in a
// platform code this bare errors.New never carries — same shape, different contract, and only one of the two
// is what production does. What this fake still buys is the part a container cannot reach: it fails ONE
// method of an arbitrary LedgerStore, so the assertion is about the Accountant's logic and nothing else.
// The contract under a genuine cut is proven in chaos_postgres_integration_test.go.
type failingStore struct {
	balanceErr   error
	recordCalled bool
	entries      cp.MessageEntries
}

func (f *failingStore) Balance(context.Context, string, uuid.UUID, string) (int, bool, error) {
	return 0, false, f.balanceErr
}

func (f *failingStore) RecordDurable(context.Context, cp.LedgerEntry) (int, bool, error) {
	f.recordCalled = true
	return 0, true, nil
}

func (f *failingStore) MessageEntries(context.Context, uuid.UUID) (cp.MessageEntries, error) {
	return f.entries, nil
}

func (f *failingStore) ReserveEntry(context.Context, uuid.UUID) (int, int, bool, error) {
	return 0, 0, false, nil
}

// TestReserveFailsClosedWhenAuthorityDown proves the fail-closed rule (§6.9) at the unit level: with the
// Redis balance cache cold and the durable read failing, a reserve is REFUSED (a credit is never passed
// unverified), no hold is placed, and nothing is mirrored to the ledger. Its sibling
// TestReserveFailsClosedWhenPostgresIsCut proves the same rule against a real severed Postgres, on all
// three of Reserve's durable paths rather than this one.
func TestReserveFailsClosedWhenAuthorityDown(t *testing.T) {
	rdb := redistest.Client(t) // real, but the owner's balance key is cold (fresh id)
	ctx := context.Background()
	authorityDown := errors.New("postgres unreachable")
	store := &failingStore{balanceErr: authorityDown}

	acc := billing.New(rdb, store)
	owner := billing.Owner{Type: cp.OwnerTypeCustomer, ID: uuid.New(), CustomerID: uuid.New()}
	messageID := uuid.New()

	_, err := acc.Reserve(ctx, owner, messageID, 3)
	if !errors.Is(err, authorityDown) {
		t.Fatalf("Reserve error = %v, want the authority-down error (fail-closed)", err)
	}
	if store.recordCalled {
		t.Error("a fail-closed reserve must NOT mirror anything to the durable store")
	}
	// No hold was placed: the reservation key must be absent.
	if n, err := rdb.Exists(ctx, "billing:reservation:"+messageID.String()).Result(); err != nil || n != 0 {
		t.Errorf("reservation key exists=%d (err=%v), want absent — no hold on a fail-closed reserve", n, err)
	}
}

// TestCaptureWithNoDurableReserveIsRefused: a capture whose message has no reserve in the ledger is an
// invariant anomaly — it is refused as a conflict and writes nothing, rather than recording a capture that
// charges for a reservation that never was.
func TestCaptureWithNoDurableReserveIsRefused(t *testing.T) {
	store := &failingStore{}
	acc := billing.New(redistest.Client(t), store)
	owner := billing.Owner{Type: cp.OwnerTypeCustomer, ID: uuid.New(), CustomerID: uuid.New()}

	_, err := acc.Capture(context.Background(), owner, uuid.New())
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("Capture error = %v, want ErrConflict: no durable reserve to capture", err)
	}
	if store.recordCalled {
		t.Error("a capture with no durable reserve must not write a ledger entry")
	}
}

// TestARedeliveredCaptureWritesNothing: the ledger already holds this message's capture, so a redelivery is
// settled on the read alone — no second write, not even one RecordDurable would turn into a no-op.
func TestARedeliveredCaptureWritesNothing(t *testing.T) {
	store := &failingStore{entries: cp.MessageEntries{Reserve: true, Capture: true}}
	acc := billing.New(redistest.Client(t), store)
	owner := billing.Owner{Type: cp.OwnerTypeCustomer, ID: uuid.New(), CustomerID: uuid.New()}

	if _, err := acc.Capture(context.Background(), owner, uuid.New()); err != nil {
		t.Fatalf("Capture of an already captured message = %v, want nil (idempotent)", err)
	}
	if store.recordCalled {
		t.Error("a redelivered capture reached RecordDurable, want it settled on the ledger read")
	}
}
