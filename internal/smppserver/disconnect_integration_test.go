package smppserver

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/session"
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestDisconnectFanOutEndToEnd proves the whole downward path over REAL Redis pub/sub: a
// SessionRegistry.Disconnect publishes an order, the smpp-server pod's subscriber receives it and
// force-closes the matching live bind, while a neighbour account's session survives. This is the seam
// the unit tests cannot cover — the actual publish→subscribe transport between two Redis clients.
func TestDisconnectFanOutEndToEnd(t *testing.T) {
	rdb := redistest.Client(t) // skips cleanly when Docker is unavailable
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// session-manager side: a Server whose Disconnect publishes to Redis.
	srv := session.NewServer(session.NewRegistry(rdb), redisstore.NewPubSubPublisher(rdb))

	// smpp-server side: a listener plus the disconnect subscriber, both against the same Redis.
	acctA, custA := uuid.New(), uuid.New()
	acctB, custB := uuid.New(), uuid.New()
	store := multiStore{creds: map[string]cp.BindCredential{
		"sys-a": wireCred(t, acctA, custA),
		"sys-b": wireCred(t, acctB, custB),
	}}
	l, addr := startTestListener(t, store, &trackedRegistry{}, Options{})

	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		_ = RunDisconnectSubscriber(ctx, redisstore.Subscribe(ctx, rdb, disconnect.Channel), l, discardLog())
	}()
	t.Cleanup(func() { cancel(); <-subDone })

	// A message published before SUBSCRIBE completes is lost (pub/sub has no backlog), so wait until
	// Redis reports the subscription is live.
	waitForSubscriber(t, rdb)

	ca := dialBind(t, addr, "sys-a", bindPW)
	cb := dialBind(t, addr, "sys-b", bindPW)

	// Publish the order through the real RPC handler (which encodes + PUBLISHes).
	if _, err := srv.Disconnect(ctx, &registrypb.DisconnectRequest{
		Scope:  registrypb.DisconnectScope_DISCONNECT_SCOPE_ACCOUNT,
		Id:     acctA.String(),
		Reason: "credential_revoked",
	}); err != nil {
		t.Fatalf("Disconnect RPC: %v", err)
	}

	expectClosed(t, ca)
	expectAlive(t, cb, 2)
}

// waitForSubscriber blocks until Redis reports at least one subscriber on the disconnect channel, so
// the publish is not lost to a not-yet-established subscription.
func waitForSubscriber(t *testing.T, rdb *redis.Client) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		counts, err := rdb.PubSubNumSub(context.Background(), disconnect.Channel).Result()
		if err == nil && counts[disconnect.Channel] >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("subscriber did not register on the disconnect channel in time")
}
