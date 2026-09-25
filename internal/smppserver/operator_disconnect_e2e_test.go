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
// it over Redis pub/sub, and the SMPP peer — not a registry key — observes the close.
func TestOperatorDisconnectFromTheAdminAPIClosesThePeer(t *testing.T) {
	rdb := redistest.Client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := session.NewServer(session.NewRegistry(rdb), redisstore.NewPubSubPublisher(rdb))
	acct := uuid.New()
	cred := wireCred(t, acct, uuid.New())
	cred.MaxSessions = 2
	l, addr := startTestListener(t, multiStore{creds: map[string]cp.BindCredential{"sys-e2e": cred}},
		registryServer{srv}, Options{})
	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		_ = RunDisconnectSubscriber(ctx, redisstore.Subscribe(ctx, rdb, disconnect.Channel), l, discardLog())
	}()
	t.Cleanup(func() { cancel(); <-subDone })
	waitForSubscriber(t, rdb)

	api := adminAPIOver(t, srv)
	first := dialBind(t, addr, "sys-e2e", bindPW)
	second := dialBind(t, addr, "sys-e2e", bindPW)

	ids := listedSessionIDs(t, api, acct)
	if len(ids) != 2 {
		t.Fatalf("listed %d sessions, want the 2 binds", len(ids))
	}

	w := httptest.NewRecorder()
	api.ServeHTTP(w, operatorRequest(http.MethodDelete, "/v1/admin/sessions/"+ids[0]))
	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204; body=%s", w.Code, w.Body)
	}
	if closesWithin(first) == closesWithin(second) {
		t.Fatal("want exactly one of the two binds closed by the operator's DELETE")
	}

	deadline := time.Now().Add(3 * time.Second)
	for len(listedSessionIDs(t, api, acct)) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the closed session never left the list")
		}
		time.Sleep(20 * time.Millisecond)
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, operatorRequest(http.MethodDelete, "/v1/admin/sessions/"+ids[0]))
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

func listedSessionIDs(t *testing.T, api http.Handler, acct uuid.UUID) []string {
	t.Helper()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, operatorRequest(http.MethodGet, "/v1/admin/sessions?accountId="+acct.String()))
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d; body=%s", w.Code, w.Body)
	}
	var page struct {
		Data []struct {
			ID        string `json:"id"`
			AccountID string `json:"account_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	ids := make([]string, 0, len(page.Data))
	for _, s := range page.Data {
		if s.AccountID != acct.String() {
			t.Fatalf("listed session of account %s under the %s filter", s.AccountID, acct)
		}
		ids = append(ids, s.ID)
	}
	return ids
}
