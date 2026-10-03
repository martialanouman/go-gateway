package billing_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/billing"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

const slowDurable = 50 * time.Millisecond

type slowDurableStore struct{ failingStore }

func (slowDurableStore) Balance(context.Context, string, uuid.UUID, string) (int, bool, error) {
	return 100, true, nil
}

func (slowDurableStore) RecordDurable(context.Context, cp.LedgerEntry) (int, bool, error) {
	time.Sleep(slowDurable)
	return 97, true, nil
}

type stageRecorder struct {
	mu     sync.Mutex
	stages map[string][]float64
}

func (r *stageRecorder) ObserveReserveStage(stage string, seconds float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stages[stage] = append(r.stages[stage], seconds)
}

func TestReserveTimesItsRedisAndDurableStages(t *testing.T) {
	rec := &stageRecorder{stages: map[string][]float64{}}
	acc := billing.New(redistest.Client(t), &slowDurableStore{}, billing.WithReserveTimer(rec))
	owner := billing.Owner{Type: cp.OwnerTypeCustomer, ID: uuid.New(), CustomerID: uuid.New()}

	if _, err := acc.Reserve(context.Background(), owner, uuid.New(), 3); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	durable, redis := rec.stages["durable"], rec.stages["redis"]
	if len(durable) != 1 || durable[0] < slowDurable.Seconds() {
		t.Errorf("durable stage = %v, want one observation of at least %v", durable, slowDurable)
	}
	if len(redis) == 0 {
		t.Fatal("redis stage never observed")
	}
	for _, s := range redis {
		if s >= slowDurable.Seconds() {
			t.Errorf("redis stage %vs carries the durable write's time", s)
		}
	}
}
