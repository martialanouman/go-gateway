package billing_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

const slowDurable = 50 * time.Millisecond

type slowDurableStore struct {
	failingStore
	err error
}

func (*slowDurableStore) Balance(context.Context, string, uuid.UUID, string) (int, bool, error) {
	return 100, true, nil
}

func (s *slowDurableStore) RecordDurable(context.Context, cp.LedgerEntry) (int, bool, error) {
	time.Sleep(slowDurable)
	return 97, true, s.err
}

type observations []float64

func (o *observations) Observe(seconds float64) { *o = append(*o, seconds) }

func TestReserveTimesItselfAndItsLedgerWrite(t *testing.T) {
	for name, durableErr := range map[string]error{"committed": nil, "failed": errors.New("postgres: timeout")} {
		t.Run(name, func(t *testing.T) {
			var total, durable observations
			acc := billing.New(redistest.Client(t), &slowDurableStore{err: durableErr}, billing.WithReserveTimers(&total, &durable))
			owner := billing.Owner{Type: cp.OwnerTypeCustomer, ID: uuid.New(), CustomerID: uuid.New()}

			_, _ = acc.Reserve(context.Background(), owner, uuid.New(), 3)

			if len(durable) != 1 || durable[0] < slowDurable.Seconds() {
				t.Errorf("durable = %v, want one observation of at least %v", durable, slowDurable)
			}
			if len(total) != 1 || total[0] < slowDurable.Seconds() {
				t.Errorf("total = %v, want one observation spanning the ledger write", total)
			}
		})
	}
}
