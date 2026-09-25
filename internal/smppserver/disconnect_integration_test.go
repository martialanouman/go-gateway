package smppserver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/session"
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
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

// registryServer lets the listener bind through the real session-manager Server, so the operator's
// session-scoped order resolves binds the listener actually registered.
type registryServer struct{ *session.Server }

func (r registryServer) Bind(ctx context.Context, in *registrypb.BindRequest, _ ...grpc.CallOption) (*registrypb.BindResponse, error) {
	return r.Server.Bind(ctx, in)
}

func (r registryServer) Unbind(ctx context.Context, in *registrypb.UnbindRequest, _ ...grpc.CallOption) (*registrypb.UnbindResponse, error) {
	return r.Server.Unbind(ctx, in)
}

// TestOperatorDisconnectClosesOneBindOfAnAccount targets a single bind while its account holds two: the
// order must reach that socket and only that one — an account-scoped close would pass a weaker test.
func TestOperatorDisconnectClosesOneBindOfAnAccount(t *testing.T) {
	rdb := redistest.Client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := session.NewServer(session.NewRegistry(rdb), redisstore.NewPubSubPublisher(rdb))
	acct, cust := uuid.New(), uuid.New()
	cred := wireCred(t, acct, cust)
	cred.MaxSessions = 2
	l, addr := startTestListener(t, multiStore{creds: map[string]cp.BindCredential{"sys-op": cred}},
		registryServer{srv}, Options{InboundWindow: 7})

	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		_ = RunDisconnectSubscriber(ctx, redisstore.Subscribe(ctx, rdb, disconnect.Channel), l, discardLog())
	}()
	t.Cleanup(func() { cancel(); <-subDone })
	waitForSubscriber(t, rdb)

	first := dialBind(t, addr, "sys-op", bindPW)
	second := dialBind(t, addr, "sys-op", bindPW)

	listed, err := srv.ListSessions(ctx, &registrypb.ListSessionsRequest{AccountId: acct.String()})
	if err != nil || len(listed.GetSessions()) != 2 {
		t.Fatalf("list: %v sessions, err=%v, want the 2 binds", len(listed.GetSessions()), err)
	}
	for _, s := range listed.GetSessions() {
		if ip := net.ParseIP(s.GetRemoteAddr()); ip == nil || !ip.IsLoopback() || s.GetWindowSize() != 7 || s.GetSystemId() != "sys-op" {
			t.Fatalf("listed session = %+v, want the pod's remote address, window and system id", s)
		}
	}

	// Which listed bind is which socket is not observable from the client, so target one and assert
	// that exactly one of the two sockets closes.
	if _, err := srv.Disconnect(ctx, &registrypb.DisconnectRequest{
		Scope:  registrypb.DisconnectScope_DISCONNECT_SCOPE_SESSION,
		Id:     listed.GetSessions()[0].GetBindId(),
		Reason: "operator_disconnect",
	}); err != nil {
		t.Fatalf("Disconnect RPC: %v", err)
	}

	closedFirst := closesWithin(first)
	closedSecond := closesWithin(second)
	if closedFirst == closedSecond {
		t.Fatalf("closed first=%v second=%v, want exactly one bind closed", closedFirst, closedSecond)
	}
}

// closesWithin reports whether the peer sees the server's unbind or EOF; a live socket answers an
// enquire_link instead.
func closesWithin(c net.Conn) bool {
	_ = c.SetWriteDeadline(time.Now().Add(ioDeadlineD))
	if err := smpp.WritePDU(c, smpp.PDU{Sequence: 9, Body: &smpp.EnquireLink{}}); err != nil {
		return true
	}
	_ = c.SetReadDeadline(time.Now().Add(ioDeadlineD))
	pdu, err := smpp.ReadPDU(c)
	return err != nil || pdu.CommandID() == smpp.CmdUnbind
}
