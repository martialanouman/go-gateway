package smppserver

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/session"
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestOperatorDisconnectFromTheAdminAPIClosesThePeer drives the whole path step-360 adds: the Admin API
// lists the account's binds through the gRPC registry, a DELETE of one of them reaches the pod holding
// it over Redis pub/sub, and the SMPP peer — not a registry key — observes the close while the same
// account's other bind keeps serving: an account-scoped close would pass a weaker test.
func TestOperatorDisconnectFromTheAdminAPIClosesThePeer(t *testing.T) {
	rdb := redistest.Client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := session.NewServer(session.NewRegistry(rdb), redisstore.NewPubSubPublisher(rdb))
	acct := uuid.New()
	cred := wireCred(t, acct, uuid.New())
	cred.MaxSessions = 2
	l, addr := startTestListener(t, multiStore{creds: map[string]cp.BindCredential{"sys-e2e": cred}},
		registryServer{srv}, Options{InboundWindow: 7})
	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		_ = RunDisconnectSubscriber(ctx, redisstore.Subscribe(ctx, rdb, disconnect.Channel), l, discardLog())
	}()
	t.Cleanup(func() { cancel(); <-subDone })
	waitForSubscriber(t, rdb)

	api := adminAPIOver(t, srv)
	// Bound one at a time, so the listing names which socket each id is.
	target := dialBind(t, addr, "sys-e2e", bindPW)
	listed := listedSessions(t, api, acct)
	if len(listed) != 1 {
		t.Fatalf("listed %d sessions, want 1", len(listed))
	}
	targetID := listed[0].ID
	if ip := net.ParseIP(listed[0].RemoteAddr); ip == nil || !ip.IsLoopback() || listed[0].WindowSize != 7 {
		t.Fatalf("listed session = %+v, want the pod's loopback remote address and window 7", listed[0])
	}
	neighbour := dialBind(t, addr, "sys-e2e", bindPW)
	if n := len(listedSessions(t, api, acct)); n != 2 {
		t.Fatalf("listed %d sessions, want the 2 binds", n)
	}

	w := httptest.NewRecorder()
	api.ServeHTTP(w, operatorRequest(http.MethodDelete, "/v1/admin/sessions/"+targetID))
	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204; body=%s", w.Code, w.Body)
	}
	expectClosed(t, target)
	expectAlive(t, neighbour, 2)

	deadline := time.Now().Add(3 * time.Second)
	for len(listedSessions(t, api, acct)) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the closed session never left the list")
		}
		time.Sleep(20 * time.Millisecond)
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, operatorRequest(http.MethodDelete, "/v1/admin/sessions/"+targetID))
	if w.Code != http.StatusNotFound {
		t.Fatalf("second DELETE of the closed session = %d, want 404", w.Code)
	}
}

const e2eOperatorToken = "e2e-operator"

func adminAPIOver(t *testing.T, srv *session.Server) http.Handler {
	t.Helper()
	gs := grpc.NewServer()
	registrypb.RegisterSessionRegistryServer(gs, srv)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	verifier, err := auth.NewStaticVerifier([]string{e2eOperatorToken + ":admin:read|admin:write"})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	api, _ := adminapi.New(adminapi.Deps{
		Verifier: verifier,
		Sessions: adminapi.NewGRPCSessions(registrypb.NewSessionRegistryClient(conn)),
	})
	return api
}

func operatorRequest(method, path string) *http.Request {
	r := httptest.NewRequest(method, path, http.NoBody)
	r.Header.Set("Authorization", "Bearer "+e2eOperatorToken)
	return r
}

type listedSession struct {
	ID         string `json:"id"`
	AccountID  string `json:"account_id"`
	RemoteAddr string `json:"remote_addr"`
	WindowSize int    `json:"window_size"`
}

func listedSessions(t *testing.T, api http.Handler, acct uuid.UUID) []listedSession {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, operatorRequest(http.MethodGet, "/v1/admin/sessions?accountId="+acct.String()))
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d; body=%s", w.Code, w.Body)
	}
	var page struct {
		Data []listedSession `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	for _, s := range page.Data {
		if s.AccountID != acct.String() {
			t.Fatalf("listed session of account %s under the %s filter", s.AccountID, acct)
		}
	}
	return page.Data
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
