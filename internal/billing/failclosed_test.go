package billing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
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
}

func (f *failingStore) Balance(context.Context, string, uuid.UUID, string) (int, bool, error) {
	return 0, false, f.balanceErr
}

func (f *failingStore) RecordDurable(context.Context, cp.LedgerEntry) (int, bool, error) {
	f.recordCalled = true
	return 0, true, nil
}

func (f *failingStore) LedgerEntryExists(context.Context, uuid.UUID, cp.EntryType) (bool, error) {
	return false, nil
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
