package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// step-283: a submit_sm beyond the account's throughput is refused before the acknowledgement, through
// the ingestion the listener is built with — the one REST shares.
func TestASubmitBeyondTheAccountRateIsRefusedBeforeTheAck(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.Kafka.Brokers = kafkatest.Brokers(t)
	pool := pgtest.Pool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	account := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO control_plane.rate_limits (entity_type, entity_id, max_per_sec, burst_capacity)
		VALUES ('smpp_account', $1, 1, 1)`, account); err != nil {
		t.Fatalf("insert rate limit: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM control_plane.rate_limits WHERE entity_id = $1`, account)
	})
	app, err := newSMPPApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newSMPPApp: %v", err)
	}
	defer app.close()

	submit := func() error {
		return app.ingestor.Accept(ctx, pipeline.InboundMT{
			MessageID: uuid.New(), TraceID: uuid.New(), AccountID: account, CustomerID: uuid.New(),
			From: "INFO", To: "+2250700000001", Body: msg.NewBodyString("hello"), Encoding: "auto",
			SubmittedAt: time.Now(),
		})
	}
	if err := submit(); err != nil {
		t.Fatalf("the first submit within the rate: %v — the control failed", err)
	}
	// The bucket refills at 1/s and the first produce can take that long on a cold broker.
	for range 5 {
		err := submit()
		if code, _ := errs.CodeOf(err); code == errs.ErrRateLimited {
			return
		}
		if err != nil {
			t.Fatalf("submit = %v, want nil or rate_limited", err)
		}
	}
	t.Fatal("five submits past the account's 1/s were all admitted, want rate_limited (ESME_RTHROTTLED)")
}
