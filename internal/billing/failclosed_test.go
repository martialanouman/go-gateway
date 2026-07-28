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

// failingStore is a LedgerStore whose durable read fails, standing in for an unreachable Postgres.
// RecordDurable records that it was reached, so a test can prove a denied reserve never mirrored anything.
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

// TestReserveFailsClosedWhenAuthorityDown proves the fail-closed rule (§6.9): with the Redis balance cache
// cold and the durable authority unreachable, a reserve is REFUSED (a credit is never passed unverified),
// no hold is placed, and nothing is mirrored to the ledger.
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
