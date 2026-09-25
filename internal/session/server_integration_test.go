package session_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"google.golang.org/genproto/googleapis/rpc/errdetails"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/session"
	"github.com/martialanouman/go-gateway/internal/session/pb"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// newTestClient wires a real Server (backed by a throwaway Redis) to an in-memory bufconn transport
// and returns a connected gRPC client. Exercising the actual gRPC codec and status machinery — not a
// direct method call — is what makes the max_sessions error's wire encoding part of the test.
func newTestClient(t *testing.T) pb.SessionRegistryClient {
	t.Helper()

	rdb := redistest.Client(t)
	srv := grpc.NewServer()
	pb.RegisterSessionRegistryServer(srv,
		session.NewServer(session.NewRegistry(rdb), redisstore.NewPubSubPublisher(rdb)))

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewSessionRegistryClient(conn)
}

func newSession(accountID string) *pb.Session {
	return &pb.Session{
		AccountId: accountID,
		SystemId:  "sys-1",
		PodId:     "pod-1",
		BindId:    "bind-" + uuid.NewString(),
		BindType:  pb.BindType_BIND_TYPE_TRX,
	}
}

// TestBindLookupUnbindRoundTrip walks the registry's happy path end to end over gRPC: a bind appears in
// a lookup, and unbinding it removes it again.
func TestBindLookupUnbindRoundTrip(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	account := uuid.NewString()
	sess := newSession(account)

	bindResp, err := client.Bind(ctx, &pb.BindRequest{Session: sess, MaxSessions: 2})
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if !bindResp.GetAccepted() || bindResp.GetActiveSessions() != 1 {
		t.Fatalf("Bind = {accepted:%t active:%d}, want {true 1}",
			bindResp.GetAccepted(), bindResp.GetActiveSessions())
	}

	lookupResp, err := client.Lookup(ctx, &pb.LookupRequest{AccountId: account})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got := lookupResp.GetSessions(); len(got) != 1 ||
		got[0].GetPodId() != sess.GetPodId() || got[0].GetBindId() != sess.GetBindId() {
		t.Fatalf("Lookup = %v, want one session with pod %q bind %q",
			got, sess.GetPodId(), sess.GetBindId())
	}

	unbindResp, err := client.Unbind(ctx, &pb.UnbindRequest{AccountId: account, BindId: sess.GetBindId()})
	if err != nil {
		t.Fatalf("Unbind: %v", err)
	}
	if !unbindResp.GetRemoved() {
		t.Fatal("Unbind removed = false, want true")
	}

	lookupResp, err = client.Lookup(ctx, &pb.LookupRequest{AccountId: account})
	if err != nil {
		t.Fatalf("Lookup after unbind: %v", err)
	}
	if got := lookupResp.GetSessions(); len(got) != 0 {
		t.Fatalf("Lookup after unbind = %v, want empty", got)
	}
}

// TestBindBeyondMaxSessionsCarriesCode is invariant (d) across the gRPC boundary: a bind over the
// ceiling is refused with a ResourceExhausted status whose ErrorInfo.Reason is the shared
// max_sessions_exceeded code, so the SMPP caller (step-024) can retranslate it into ESME_RBINDFAIL.
func TestBindBeyondMaxSessionsCarriesCode(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	account := uuid.NewString()

	if _, err := client.Bind(ctx, &pb.BindRequest{Session: newSession(account), MaxSessions: 1}); err != nil {
		t.Fatalf("first bind: %v", err)
	}

	_, err := client.Bind(ctx, &pb.BindRequest{Session: newSession(account), MaxSessions: 1})
	if err == nil {
		t.Fatal("second bind over quota succeeded, want a rejection")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a gRPC status", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("status code = %s, want ResourceExhausted", st.Code())
	}

	var reason string
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			reason = info.GetReason()
		}
	}
	if reason != errs.ErrMaxSessionsExceeded.String() {
		t.Errorf("ErrorInfo.Reason = %q, want %q", reason, errs.ErrMaxSessionsExceeded.String())
	}
}

// TestDeliverIsUnimplemented pins the deferred MO return path: Deliver must fail loudly with
// Unimplemented rather than pretend a delivery happened, until step-046/048 land.
func TestDeliverIsUnimplemented(t *testing.T) {
	client := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Deliver(ctx, &pb.DeliverRequest{BindId: "bind-1", Pdu: []byte{0x00}})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Deliver code = %s, want Unimplemented (err=%v)", status.Code(err), err)
	}
}

// TestBindLookupCarriesPodAddrAcrossGRPC is step-302 across the wire: the address a pod publishes at
// bind time must come back on Lookup, because that is the only thing mo-dlr-router-svc has left to
// dial — there is no template to compose a name from any more.
func TestBindLookupCarriesPodAddrAcrossGRPC(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	account := uuid.NewString()
	sess := newSession(account)
	sess.PodAddr = "10.4.5.6:7000"

	if _, err := client.Bind(ctx, &pb.BindRequest{Session: sess, MaxSessions: 2}); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	lookupResp, err := client.Lookup(ctx, &pb.LookupRequest{AccountId: account})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	got := lookupResp.GetSessions()
	if len(got) != 1 {
		t.Fatalf("Lookup = %d sessions, want 1", len(got))
	}
	if got[0].GetPodAddr() != sess.GetPodAddr() {
		t.Errorf("Lookup pod_addr = %q, want %q — without it the router has no address to dial and "+
			"every MO falls through to the webhook", got[0].GetPodAddr(), sess.GetPodAddr())
	}
}

func TestListSessionsDescribesEachBindAcrossGRPC(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	account := uuid.NewString()

	sess := newSession(account)
	sess.RemoteAddr = "198.51.100.4"
	sess.WindowSize = 12
	if _, err := client.Bind(ctx, &pb.BindRequest{Session: sess, MaxSessions: 2}); err != nil {
		t.Fatalf("bind: %v", err)
	}

	resp, err := client.ListSessions(ctx, &pb.ListSessionsRequest{AccountId: account})
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if resp.GetActive() != 1 || len(resp.GetSessions()) != 1 {
		t.Fatalf("active=%d sessions=%d, want 1 and 1", resp.GetActive(), len(resp.GetSessions()))
	}
	got := resp.GetSessions()[0]
	if got.GetBindId() != sess.GetBindId() || got.GetSystemId() != "sys-1" || got.GetPodId() != "pod-1" ||
		got.GetBindType() != pb.BindType_BIND_TYPE_TRX || got.GetRemoteAddr() != "198.51.100.4" ||
		got.GetWindowSize() != 12 || got.GetConnectedAtUnixMs() == 0 {
		t.Fatalf("listed session = %+v, want the bind as declared, stamped", got)
	}

	page, err := client.ListSessions(ctx, &pb.ListSessionsRequest{Cursor: sess.GetBindId()[:len(sess.GetBindId())-1], Limit: 1})
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if len(page.GetSessions()) != 1 || page.GetSessions()[0].GetBindId() != sess.GetBindId() {
		t.Fatalf("page = %+v, want the bind just after the cursor", page.GetSessions())
	}
}

func TestListSessionsRefusesALimitOutsideTheContract(t *testing.T) {
	client := newTestClient(t)
	for _, limit := range []int32{0, 501} {
		_, err := client.ListSessions(context.Background(), &pb.ListSessionsRequest{Limit: limit})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("list with limit %d: code=%v, want InvalidArgument", limit, status.Code(err))
		}
	}
}

// TestUnbindOfALapsedBindStillUnindexesIt: a bind whose token lapsed before its unbind is no longer in
// the account key, and its index entry must not outlive it for want of a reader.
func TestUnbindOfALapsedBindStillUnindexesIt(t *testing.T) {
	rdb := redistest.Client(t)
	clk := &clock{t: time.Unix(1_700_000_000, 0)}
	srv := session.NewServer(session.NewRegistry(rdb, session.WithSessionTTL(30*time.Second), session.WithClock(clk.now)), nil)
	ctx := context.Background()
	sess := newSession(uuid.NewString())
	if _, err := srv.Bind(ctx, &pb.BindRequest{Session: sess, MaxSessions: 1}); err != nil {
		t.Fatalf("bind: %v", err)
	}
	clk.advance(31 * time.Second)

	if _, err := srv.Unbind(ctx, &pb.UnbindRequest{AccountId: sess.GetAccountId(), BindId: sess.GetBindId()}); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if n := indexEntries(t, rdb, sess.GetBindId()); n != 0 {
		t.Fatalf("index holds %d entries for an unbound bind, want 0", n)
	}
}

func TestDisconnectSessionAnswersNotFoundForAnUnknownBind(t *testing.T) {
	client := newTestClient(t)
	_, err := client.Disconnect(context.Background(), &pb.DisconnectRequest{
		Scope: pb.DisconnectScope_DISCONNECT_SCOPE_SESSION, Id: "bind-" + uuid.NewString(), Reason: "operator_disconnect",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("disconnect unknown session: code=%v, want NotFound", status.Code(err))
	}
}

// TestUnbindUnderAnotherAccountLeavesTheIndexAlone: an unbind naming the wrong account must not unlist a
// live bind it does not own.
func TestUnbindUnderAnotherAccountLeavesTheIndexAlone(t *testing.T) {
	rdb := redistest.Client(t)
	srv := session.NewServer(session.NewRegistry(rdb), nil)
	ctx := context.Background()
	sess := newSession(uuid.NewString())
	if _, err := srv.Bind(ctx, &pb.BindRequest{Session: sess, MaxSessions: 1}); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if _, err := srv.Unbind(ctx, &pb.UnbindRequest{AccountId: uuid.NewString(), BindId: sess.GetBindId()}); err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if n := indexEntries(t, rdb, sess.GetBindId()); n != 1 {
		t.Fatalf("index holds %d entries for the live bind, want 1", n)
	}
}
