package billing_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestStrictPrepaidNeverOverdrawsWhileFolding is the step-284 DoD under load: reserves race the fold and a
// cache that keeps expiring, so every few milliseconds a reserve rehydrates from a durable balance whose
// deltas are half folded. A strict-prepaid customer must never get one credit more than it paid for.
func TestStrictPrepaidNeverOverdrawsWhileFolding(t *testing.T) {
	const funded, workers, perWorker = 50, 20, 10
	h := newBillingHarness(t, funded)
	ctx := context.Background()

	stop := make(chan struct{})
	var background sync.WaitGroup
	tick := func(every time.Duration, fn func()) {
		background.Add(1)
		go func() {
			defer background.Done()
			ticker := time.NewTicker(every)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					fn()
				}
			}
		}()
	}
	tick(time.Millisecond, func() {
		if _, err := h.verify.FoldOnce(ctx, 3); err != nil {
			t.Errorf("FoldOnce: %v", err)
		}
	})
	tick(5*time.Millisecond, func() {
		if err := h.rdb.Del(ctx, "billing:balance:mt:"+h.owner.Type+":"+h.owner.ID.String()).Err(); err != nil {
			t.Errorf("drop cache: %v", err)
		}
	})

	var accepted atomic.Int64
	var reservers sync.WaitGroup
	for range workers {
		reservers.Add(1)
		go func() {
			defer reservers.Done()
			for range perWorker {
				_, err := h.acc.Reserve(ctx, h.owner, uuid.New(), 1)
				switch {
				case err == nil:
					accepted.Add(1)
				case !errors.Is(err, errs.ErrInsufficientCredit):
					t.Errorf("Reserve: %v", err)
				}
			}
		}()
	}
	reservers.Wait()
	close(stop)
	background.Wait()

	if got := accepted.Load(); got != funded {
		t.Errorf("accepted %d reserves of 1 credit against %d funded, want exactly %d", got, funded, funded)
	}
	if got := h.balance(t); got != 0 {
		t.Errorf("durable balance = %d, want 0", got)
	}
	var sum int
	if err := pgtest.Pool(t).QueryRow(ctx,
		`SELECT coalesce(sum(credits), 0) FROM control_plane.billing_ledger WHERE owner_type = $1 AND owner_id = $2`,
		h.owner.Type, h.owner.ID).Scan(&sum); err != nil {
		t.Fatalf("sum ledger: %v", err)
	}
	if sum != 0 {
		t.Errorf("SUM(ledger credits) = %d, want 0 (it must equal the balance)", sum)
	}
}
