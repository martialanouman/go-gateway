package smppserver_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/session"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
	smppsession "github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	redisstore "github.com/martialanouman/go-gateway/internal/storage/redis"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestBindValidAndRejections drives real ESME binds through the listener against a real control-plane
// database and a real SessionRegistry (Redis-backed, over gRPC).
func TestBindValidAndRejections(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)

	t.Run("valid transceiver bind is accepted", func(t *testing.T) {
		sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
		addr := startListener(t, pool, registry)

		e := dialESME(t, addr)
		defer e.close()
		if got := e.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
			t.Fatalf("bind status = %#x, want ESME_ROK", got)
		}
	})

	t.Run("wrong password is ESME_RINVPASWD", func(t *testing.T) {
		sid, _, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
		addr := startListener(t, pool, registry)

		e := dialESME(t, addr)
		defer e.close()
		// A wrong but SMPP-legal (<=8 octet) password: the codec accepts the frame, auth rejects the value.
		if got := e.bind(t, smppsession.BindTransceiver, sid, "wrongpw"); got != errs.StatusInvalidPasswd {
			t.Fatalf("bind status = %#x, want ESME_RINVPASWD", got)
		}
	})

	t.Run("unknown system_id is ESME_RINVPASWD", func(t *testing.T) {
		addr := startListener(t, pool, registry)

		e := dialESME(t, addr)
		defer e.close()
		if got := e.bind(t, smppsession.BindTransceiver, "nobody", "whatever"); got != errs.StatusInvalidPasswd {
			t.Fatalf("bind status = %#x, want ESME_RINVPASWD", got)
		}
	})

	t.Run("bind type outside allowed_bind_types is ESME_RBINDFAIL", func(t *testing.T) {
		sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTX})
		addr := startListener(t, pool, registry)

		e := dialESME(t, addr)
		defer e.close()
		// The account allows tx only; a transceiver bind must be refused.
		if got := e.bind(t, smppsession.BindTransceiver, sid, pw); got != errs.StatusBindFail {
			t.Fatalf("bind status = %#x, want ESME_RBINDFAIL", got)
		}
	})

	t.Run("smpp_enabled=false is ESME_RBINDFAIL", func(t *testing.T) {
		sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX, smppDisabled: true})
		addr := startListener(t, pool, registry)

		e := dialESME(t, addr)
		defer e.close()
		if got := e.bind(t, smppsession.BindTransceiver, sid, pw); got != errs.StatusBindFail {
			t.Fatalf("bind status = %#x, want ESME_RBINDFAIL", got)
		}
	})
}

// TestInvariantDMaxSessions proves invariant (d) end to end: a bind beyond max_sessions is refused,
// and an unbind frees the slot so a later bind succeeds — against the real Redis-backed registry.
func TestInvariantDMaxSessions(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)

	sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
	addr := startListener(t, pool, registry)

	// First bind fills the single slot.
	first := dialESME(t, addr)
	defer first.close()
	if got := first.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("first bind status = %#x, want ESME_ROK", got)
	}

	// Second concurrent bind is over the quota — invariant (d).
	second := dialESME(t, addr)
	if got := second.bind(t, smppsession.BindTransceiver, sid, pw); got != errs.StatusBindFail {
		t.Fatalf("second bind status = %#x, want ESME_RBINDFAIL", got)
	}
	second.close()

	// Unbind the first: the listener releases its registry token after the session ends.
	first.unbind(t)
	first.close()

	// A fresh bind now succeeds, proving the slot was freed. The release is asynchronous (it runs after
	// the session goroutine returns), so retry briefly until the slot is observably free.
	if got := eventuallyBind(t, addr, sid, pw); got != smpp.StatusOK {
		t.Fatalf("third bind after unbind status = %#x, want ESME_ROK", got)
	}
}

// TestBindWithGeneratedCredential is the regression guard for the bug where GenerateBindPassword issued
// a 32-char password that no ESME could send (the SMPP password field caps at 8 chars). It seeds a
// credential with the real generator and binds with the exact secret it returned.
func TestBindWithGeneratedCredential(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)

	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "Co-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	// The password comes from the production generator — the whole point of the regression is that its
	// output is SMPP-legal and therefore bindable.
	password, hash, err := credential.GenerateBindPassword()
	if err != nil {
		t.Fatalf("generate bind password: %v", err)
	}
	systemID := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := creds.Create(ctx, cp.NewCredential{
		AccountID:    account.ID,
		Type:         cp.CredentialSMPPBind,
		SystemID:     &systemID,
		PasswordHash: &hash,
	}); err != nil {
		t.Fatalf("create credential: %v", err)
	}

	addr := startListener(t, pool, registry)
	e := dialESME(t, addr)
	defer e.close()
	if got := e.bind(t, smppsession.BindTransceiver, systemID, password); got != smpp.StatusOK {
		t.Fatalf("bind with generated password status = %#x, want ESME_ROK", got)
	}
}

// --- helpers ---

type seedOpts struct {
	maxSessions  int
	bindType     cp.BindType
	smppDisabled bool
}

// seedBind creates a customer, an SMPP account (with the given quota, bind type and channel state) and
// its bind credential, returning the system_id and clear-text password an ESME uses to bind.
func seedBind(t *testing.T, pool *pgxpool.Pool, opts seedOpts) (systemID, password string, accountID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	customers := postgres.NewCustomerRepo(pool)
	accounts := postgres.NewAccountRepo(pool)
	creds := postgres.NewCredentialRepo(pool)

	customer, err := customers.Create(ctx, cp.NewCustomer{Name: "Co-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := accounts.Create(ctx, cp.NewAccount{CustomerID: customer.ID, Name: "app-" + uuid.NewString()})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := accounts.SetSessionLimits(ctx, account.ID, opts.maxSessions, opts.bindType); err != nil {
		t.Fatalf("set session limits: %v", err)
	}
	if opts.smppDisabled {
		if _, err := accounts.SetChannels(ctx, account.ID, false, true); err != nil {
			t.Fatalf("disable smpp channel: %v", err)
		}
	}

	// SMPP bounds the bind fields tightly (SMPP v3.4 §4.1): system_id is a C-Octet String of at most 16
	// octets (15 chars) and password at most 9 (8 chars). The test data must fit, or the codec rejects
	// the bind frame before it ever reaches auth.
	password = "bindpw12"
	hash, err := credential.HashBindPassword(password)
	if err != nil {
		t.Fatalf("hash bind password: %v", err)
	}
	systemID = strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := creds.Create(ctx, cp.NewCredential{
		AccountID:    account.ID,
		Type:         cp.CredentialSMPPBind,
		SystemID:     &systemID,
		PasswordHash: &hash,
	}); err != nil {
		t.Fatalf("create credential: %v", err)
	}
	return systemID, password, account.ID
}

// startRegistry serves the real SessionRegistry (Redis-backed) on an ephemeral gRPC port and returns a
// client for it, so binds are enforced across the wire exactly as in production.
func startRegistry(t *testing.T, rdb *redis.Client) registrypb.SessionRegistryClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen registry: %v", err)
	}
	srv := grpc.NewServer()
	registrypb.RegisterSessionRegistryServer(srv,
		session.NewServer(session.NewRegistry(rdb), redisstore.NewPubSubPublisher(rdb)))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial registry: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return registrypb.NewSessionRegistryClient(conn)
}

// startListener runs a Listener on an ephemeral SMPP port and returns its resolved address.
func startListener(t *testing.T, pool *pgxpool.Pool, registry registrypb.SessionRegistryClient) string {
	addr, _ := startListenerRef(t, pool, registry)
	return addr
}

// startListenerRef starts the listener and returns both its address and the *Listener, so a test can
// reach its pod-local surfaces (e.g. Deliver, step-046).
func startListenerRef(t *testing.T, pool *pgxpool.Pool, registry registrypb.SessionRegistryClient) (string, *smppserver.Listener) {
	t.Helper()
	l := smppserver.New(postgres.NewBindRepo(pool), registry, nil, smppserver.Options{
		Addr:     "127.0.0.1:0",
		PodID:    "pod-test",
		SystemID: "smpp-server-svc",
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()

	addrCtx, addrCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer addrCancel()
	addr, err := l.Addr(addrCtx)
	if err != nil {
		t.Fatalf("listener addr: %v", err)
	}
	return addr, l
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// esme is a minimal SMPP ESME test client over the internal/smpp codec.
type esme struct {
	conn net.Conn
	seq  uint32
}

func dialESME(t *testing.T, addr string) *esme {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial esme: %v", err)
	}
	return &esme{conn: conn}
}

func (e *esme) bind(t *testing.T, mode smppsession.BindMode, systemID, password string) uint32 {
	t.Helper()
	e.seq++
	fields := smpp.BindFields{SystemID: systemID, Password: password, InterfaceVersion: smpp.InterfaceVersion34}
	var body smpp.Body
	switch mode {
	case smppsession.BindTransmitter:
		body = &smpp.BindTransmitter{BindFields: fields}
	case smppsession.BindReceiver:
		body = &smpp.BindReceiver{BindFields: fields}
	default:
		body = &smpp.BindTransceiver{BindFields: fields}
	}
	if err := smpp.WritePDU(e.conn, smpp.PDU{Sequence: e.seq, Body: body}); err != nil {
		t.Fatalf("write bind: %v", err)
	}
	resp, err := smpp.ReadPDU(e.conn)
	if err != nil {
		t.Fatalf("read bind resp: %v", err)
	}
	return resp.Status
}

func (e *esme) unbind(t *testing.T) {
	t.Helper()
	e.seq++
	if err := smpp.WritePDU(e.conn, smpp.PDU{Sequence: e.seq, Body: &smpp.Unbind{}}); err != nil {
		t.Fatalf("write unbind: %v", err)
	}
	if _, err := smpp.ReadPDU(e.conn); err != nil {
		t.Fatalf("read unbind resp: %v", err)
	}
}

func (e *esme) close() { _ = e.conn.Close() }

// eventuallyBind retries a fresh bind until it is accepted or the deadline passes, to absorb the
// asynchronous token release after an unbind.
func eventuallyBind(t *testing.T, addr, systemID, password string) uint32 {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last uint32
	for time.Now().Before(deadline) {
		e := dialESME(t, addr)
		last = e.bind(t, smppsession.BindTransceiver, systemID, password)
		e.close()
		if last == smpp.StatusOK {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	return last
}
