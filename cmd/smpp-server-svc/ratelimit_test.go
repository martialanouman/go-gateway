package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/config"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
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

// step-289: a sender ID's own limit reaches a running SMPP pod through its watcher, like the REST one.
func TestASenderLimitSetAfterBootReachesTheSMPPAdmission(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	cfg.Kafka.Brokers = kafkatest.Brokers(t)
	pool := pgtest.Pool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{Name: "289-smpp-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	var otp uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO control_plane.sender_ids (customer_id, address, status)
		VALUES ($1, 'OTP', 'active') RETURNING id`, customer.ID).Scan(&otp); err != nil {
		t.Fatalf("register sender: %v", err)
	}
	app, err := newSMPPApp(ctx, cfg, silentLogger())
	if err != nil {
		t.Fatalf("newSMPPApp: %v", err)
	}
	watchCtx, stopWatch := context.WithCancel(ctx)
	watcherDone := make(chan error, 1)
	go func() { watcherDone <- app.watcher.Run(watchCtx) }()
	t.Cleanup(func() {
		stopWatch()
		<-watcherDone
		app.close()
	})

	if _, err := pool.Exec(ctx, `INSERT INTO control_plane.rate_limits (entity_type, entity_id, max_per_sec, burst_capacity)
		VALUES ('sender_id', $1, 1, 1)`, otp); err != nil {
		t.Fatalf("set the sender's limit: %v", err)
	}
	pub := redisstore.NewPubSubPublisher(redistest.Client(t))
	deadline := time.Now().Add(20 * time.Second)
	for {
		err := app.ingestor.Accept(ctx, pipeline.InboundMT{
			MessageID: uuid.New(), TraceID: uuid.New(), AccountID: uuid.New(), CustomerID: customer.ID,
			From: "OTP", To: "+2250700000001", Body: msg.NewBodyString("hello"), Encoding: "auto",
			SubmittedAt: time.Now(),
		})
		if code, _ := errs.CodeOf(err); code == errs.ErrRateLimited {
			return
		}
		if err != nil {
			t.Fatalf("submit = %v, want nil or rate_limited", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the running SMPP pod never refused a sender past its 1/s limit, want rate_limited (ESME_RTHROTTLED)")
		}
		if err := pub.Publish(ctx, config.ChannelSnapshotInvalidation, []byte(`{"reason":"config"}`)); err != nil {
			t.Fatalf("publish invalidation: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
