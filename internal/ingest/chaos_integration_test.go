package ingest_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/pipeline/ratelimit"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

type stubRates struct{ entries []cp.RateLimitEntry }

func (s stubRates) List(context.Context) ([]cp.RateLimitEntry, error) { return s.entries, nil }

type stubConns struct{}

func (stubConns) List(context.Context) ([]cp.Connector, error) { return nil, nil }

// TestRedisOutageAccountsForEverySubmission is the step-250 "zéro perte" criterion, moved to where the
// throughput decision now lives (step-283): the admission. A refused submission is answered 429 or
// ESME_RTHROTTLED and the client keeps it; what the criterion forbids is a submission that is neither
// written nor refused. So the assertion is a reconciliation, submission by submission: written to
// mt.inbound, or refused rate_limited, exactly once.
//
// The batch is sent TWICE. With the clock frozen, a single batch under a cut Redis admits exactly what a
// healthy bucket would, so it would prove nothing about the cut. The first batch empties the shared
// bucket; the second can then only be admitted by the per-pod ceiling, a separate budget by design.
func TestRedisOutageAccountsForEverySubmission(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)

	account := uuid.New()
	const perSec = 4
	perSecLimit := perSec
	snap, err := ratelimit.LoadSnapshot(context.Background(), stubRates{[]cp.RateLimitEntry{{
		EntityType: ratelimit.EntityAccount, EntityID: account,
		Limit: cp.RateLimit{MaxPerSec: &perSecLimit},
	}}}, stubConns{})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	frozen := time.Now()
	enforcer := ratelimit.NewEnforcer(snap, ratelimit.NewLimiter(rdb, ratelimit.WithClock(func() time.Time { return frozen })))
	producer := &countingProducer{}
	ingestor := ingest.NewIngestor(producer, enforcer, nil)

	const batch = 12
	outcome := make(map[uuid.UUID]string, 2*batch)
	sendBatch := func() {
		t.Helper()
		for range batch {
			env := envelope("hello")
			env.AccountID = account
			err := ingestor.Accept(context.Background(), env)
			switch code, _ := errs.CodeOf(err); {
			case err == nil:
				outcome[env.MessageID] = "written"
			case code == errs.ErrRateLimited:
				outcome[env.MessageID] = "refused"
			default:
				t.Errorf("submission %s failed with %v: neither written nor refused", env.MessageID, err)
			}
		}
	}

	sendBatch()
	healthyWritten := len(producer.produced)
	if healthyWritten != perSec {
		t.Fatalf("with redis up, wrote %d of %d, want the configured limit %d — the control failed",
			healthyWritten, batch, perSec)
	}

	proxy.Cut()
	sendBatch()

	written := make(map[uuid.UUID]int, len(producer.produced))
	for _, rec := range producer.produced {
		in, err := pipeline.DecodeInbound(rec)
		if err != nil {
			t.Fatalf("decode inbound: %v", err)
		}
		written[in.MessageID]++
	}
	for id, n := range written {
		if n > 1 {
			t.Errorf("submission %s was written %d times", id, n)
		}
		if outcome[id] != "written" {
			t.Errorf("submission %s reached mt.inbound but was answered %q", id, outcome[id])
		}
	}
	for id, answer := range outcome {
		if answer == "written" && written[id] == 0 {
			t.Errorf("submission %s was acknowledged and never written: the loss the criterion forbids", id)
		}
	}
	if len(outcome) != 2*batch {
		t.Errorf("accounted for %d of %d submissions", len(outcome), 2*batch)
	}

	// The outage half must come from the per-pod ceiling and must still be bounded by it: 0 means the
	// fallback never engaged, `batch` that it stopped limiting.
	if outageWritten := len(producer.produced) - healthyWritten; outageWritten != perSec {
		t.Errorf("wrote %d of %d during the outage, want exactly the per-pod ceiling %d", outageWritten, batch, perSec)
	}
}
