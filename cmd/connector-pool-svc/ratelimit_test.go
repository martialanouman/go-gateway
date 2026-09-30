package main

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// step-283: the connector's ceiling left the router for the send. A pool built without its SendLimiter
// would send at whatever rate mt.routed delivers, and nothing else in the graph would show it.
func TestThePoolPacesItsSendsOnTheSharedConnectorBucket(t *testing.T) {
	cfg := testConfig()
	cfg.Postgres = pgtest.Config(t)
	cfg.Redis = redistest.Config(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	app, err := newPoolApp(ctx, cfg, testBindEnv(), silentLogger())
	if err != nil {
		t.Fatalf("newPoolApp: %v", err)
	}
	defer app.close()

	wired := reflect.ValueOf(app.pool).Elem().FieldByName("deps").FieldByName("SendLimiter")
	if !wired.IsValid() || wired.IsNil() || wired.Elem().Type().String() != "*ratelimit.Enforcer" {
		t.Error("the pool sends without waiting on the connector's shared bucket")
	}
}
