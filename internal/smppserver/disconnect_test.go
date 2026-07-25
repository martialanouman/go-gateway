package smppserver

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// ioDeadlineD bounds each client read/write in these listener-level tests.
const ioDeadlineD = 3 * time.Second

// bindPW is the wire password these tests bind with. SMPP caps password at 8 octets (unlike the
// authorize-only unit tests, these encode the bind onto the socket), so it must stay within the limit.
const bindPW = "pw123456"

// multiStore is a CredentialStore resolving each system_id to its own credential, so a test can bind
// several distinct accounts against one listener.
type multiStore struct {
	creds map[string]cp.BindCredential
}

func (s multiStore) BindCredentialBySystemID(_ context.Context, systemID string) (cp.BindCredential, bool, error) {
	c, ok := s.creds[systemID]
	return c, ok, nil
}

// trackedRegistry is a thread-safe fake registry that always accepts and counts unbinds, so a test can
// assert a force-closed session's token is released (the max_sessions slot is freed).
type trackedRegistry struct {
	mu      sync.Mutex
	unbinds int
}

func (r *trackedRegistry) Bind(context.Context, *registrypb.BindRequest, ...grpc.CallOption) (*registrypb.BindResponse, error) {
	return &registrypb.BindResponse{Accepted: true, ActiveSessions: 1}, nil
}

func (r *trackedRegistry) Unbind(context.Context, *registrypb.UnbindRequest, ...grpc.CallOption) (*registrypb.UnbindResponse, error) {
	r.mu.Lock()
	r.unbinds++
	r.mu.Unlock()
	return &registrypb.UnbindResponse{}, nil
}

func (r *trackedRegistry) unbindCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.unbinds
}

// startTestListener runs a Listener on an ephemeral port with the given store/registry/opts and returns
// its resolved address. Everything is torn down on cleanup.
func startTestListener(t *testing.T, store CredentialStore, reg Registry, opts Options) (*Listener, string) {
	t.Helper()
	opts.Addr = ":0"
	l := New(store, reg, nil, opts, discardLog())

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- l.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-errc
	})

	addr, err := l.Addr(ctx)
	if err != nil {
		t.Fatalf("resolve listener addr: %v", err)
	}
	return l, addr
}

// dialBind opens a transceiver bind on addr with the given credentials and returns the connected
// client. It fails the test if the bind is not accepted.
func dialBind(t *testing.T, addr, systemID, password string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = c.Close() })

	_ = c.SetWriteDeadline(time.Now().Add(ioDeadlineD))
	bind := smpp.PDU{Sequence: 1, Body: &smpp.BindTransceiver{BindFields: smpp.BindFields{
		SystemID: systemID, Password: password, InterfaceVersion: smpp.InterfaceVersion34,
	}}}
	if err := smpp.WritePDU(c, bind); err != nil {
		t.Fatalf("write bind: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(ioDeadlineD))
	resp, err := smpp.ReadPDU(c)
	if err != nil {
		t.Fatalf("read bind resp: %v", err)
	}
	if resp.Status != smpp.StatusOK {
		t.Fatalf("bind %s status = %#x, want StatusOK", systemID, resp.Status)
	}
	return c
}

// expectClosed asserts the client observes the server-initiated close: an outbound unbind followed by
// EOF (or a bare EOF if the courtesy unbind lost the race with the socket close). Either way the
// session is gone.
func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(ioDeadlineD))
	pdu, err := smpp.ReadPDU(c)
	if err != nil {
		return // socket already closed — the disconnect happened
	}
	if pdu.CommandID() != smpp.CmdUnbind {
		t.Fatalf("first frame after disconnect = %#x, want unbind or EOF", pdu.CommandID())
	}
	// After the courtesy unbind the socket closes; a follow-up read observes it.
	_ = c.SetReadDeadline(time.Now().Add(ioDeadlineD))
	if _, err := smpp.ReadPDU(c); err == nil {
		t.Error("expected EOF after the server-initiated unbind")
	}
}

// expectAlive asserts the session is still serving: an enquire_link is answered.
func expectAlive(t *testing.T, c net.Conn, seq uint32) {
	t.Helper()
	_ = c.SetWriteDeadline(time.Now().Add(ioDeadlineD))
	if err := smpp.WritePDU(c, smpp.PDU{Sequence: seq, Body: &smpp.EnquireLink{}}); err != nil {
		t.Fatalf("write enquire_link: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(ioDeadlineD))
	resp, err := smpp.ReadPDU(c)
	if err != nil {
		t.Fatalf("read enquire_link resp: %v", err)
	}
	if resp.CommandID() != smpp.CmdEnquireLinkResp {
		t.Fatalf("neighbour session not alive: resp = %#x", resp.CommandID())
	}
}

// wireCred returns an active transceiver credential for the given account and customer whose password
// is the wire-safe bindPW (≤ 8 octets), so a bind that encodes it onto the socket authenticates.
func wireCred(t *testing.T, accountID, customerID uuid.UUID) cp.BindCredential {
	t.Helper()
	hash, err := credential.HashBindPassword(bindPW)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return cp.BindCredential{
		PasswordHash:     hash,
		CredentialStatus: cp.CredentialActive,
		SMPPEnabled:      true,
		AllowedBindType:  cp.BindTRX,
		MaxSessions:      1,
		AccountStatus:    cp.AccountActive,
		CustomerStatus:   cp.CustomerActive,
		AccountID:        accountID,
		CustomerID:       customerID,
	}
}

// TestDisconnectClosesAccountAndSparesNeighbour drives two live binds on different accounts, then a
// Disconnect scoped to the first account: its session is force-closed and its registry token released,
// while the second account's still-valid session keeps serving.
func TestDisconnectClosesAccountAndSparesNeighbour(t *testing.T) {
	acctA, custA := uuid.New(), uuid.New()
	acctB, custB := uuid.New(), uuid.New()
	store := multiStore{creds: map[string]cp.BindCredential{
		"sys-a": wireCred(t, acctA, custA),
		"sys-b": wireCred(t, acctB, custB),
	}}
	reg := &trackedRegistry{}
	l, addr := startTestListener(t, store, reg, Options{})

	ca := dialBind(t, addr, "sys-a", bindPW)
	cb := dialBind(t, addr, "sys-b", bindPW)

	l.Disconnect(disconnect.ScopeAccount, acctA.String(), "credential_revoked")

	expectClosed(t, ca)
	expectAlive(t, cb, 2)

	// The force-closed session released its token (the max_sessions slot is freed); the survivor did not.
	if got := reg.unbindCount(); got != 1 {
		t.Errorf("registry unbinds = %d, want 1 (only the closed session)", got)
	}
}

// TestDisconnectCustomerScopeClosesAllAccounts drives two binds under the same customer (distinct
// accounts) and asserts a customer-scoped Disconnect closes both.
func TestDisconnectCustomerScopeClosesAllAccounts(t *testing.T) {
	cust := uuid.New()
	acct1, acct2 := uuid.New(), uuid.New()
	store := multiStore{creds: map[string]cp.BindCredential{
		"sys-1": wireCred(t, acct1, cust),
		"sys-2": wireCred(t, acct2, cust),
	}}
	l, addr := startTestListener(t, store, &trackedRegistry{}, Options{})

	c1 := dialBind(t, addr, "sys-1", bindPW)
	c2 := dialBind(t, addr, "sys-2", bindPW)

	l.Disconnect(disconnect.ScopeCustomer, cust.String(), "customer_suspended")

	expectClosed(t, c1)
	expectClosed(t, c2)
}

// TestDisconnectIsIdempotent asserts a second disconnect order for an already-closed session does not
// error or panic.
func TestDisconnectIsIdempotent(t *testing.T) {
	acct, cust := uuid.New(), uuid.New()
	store := multiStore{creds: map[string]cp.BindCredential{"sys-a": wireCred(t, acct, cust)}}
	l, addr := startTestListener(t, store, &trackedRegistry{}, Options{})

	c := dialBind(t, addr, "sys-a", bindPW)
	l.Disconnect(disconnect.ScopeAccount, acct.String(), "credential_revoked")
	expectClosed(t, c)
	// Second order: the session is already gone (and likely deregistered) — must be a no-op.
	l.Disconnect(disconnect.ScopeAccount, acct.String(), "credential_revoked")
}

// TestGraceCutoffClosesLiveSession pins the post-grace cutoff on a LIVE session (not just at bind): a
// bind authenticated with the previous secret during the grace window is force-closed when the window
// ends. The grace-open decision uses the injected clock; the cutoff is a real timer sized to a tiny
// grace window, and the test awaits the actual close via a blocking read rather than sleeping.
func TestGraceCutoffClosesLiveSession(t *testing.T) {
	graceNow := time.Now()
	deadline := graceNow.Add(80 * time.Millisecond)

	oldHash, err := credential.HashBindPassword("old1234")
	if err != nil {
		t.Fatalf("hash old secret: %v", err)
	}
	cred := wireCred(t, uuid.New(), uuid.New()) // PasswordHash = bindPW (the NEW secret)
	cred.PreviousSecretHash = &oldHash
	cred.GraceExpiresAt = &deadline

	store := multiStore{creds: map[string]cp.BindCredential{"sys-a": cred}}
	reg := &trackedRegistry{}
	_, addr := startTestListener(t, store, reg, Options{Now: func() time.Time { return graceNow }})

	// Bind with the OLD secret: it authenticates only through the still-open grace window, arming the
	// cutoff at the deadline.
	c := dialBind(t, addr, "sys-a", "old1234")

	// The cutoff fires at the deadline; the read blocks until it does (no sleep).
	expectClosed(t, c)
	if got := reg.unbindCount(); got != 1 {
		t.Errorf("registry unbinds = %d, want 1 (grace cutoff released the token)", got)
	}
}

// TestGraceCutoffSparesNewSecretSession pins that the grace cutoff never touches a session that
// authenticated with the current secret: it has no cutoff armed and keeps serving past the window.
func TestGraceCutoffSparesNewSecretSession(t *testing.T) {
	graceNow := time.Now()
	deadline := graceNow.Add(40 * time.Millisecond)

	oldHash, err := credential.HashBindPassword("old1234")
	if err != nil {
		t.Fatalf("hash old secret: %v", err)
	}
	cred := wireCred(t, uuid.New(), uuid.New())
	cred.PreviousSecretHash = &oldHash
	cred.GraceExpiresAt = &deadline

	store := multiStore{creds: map[string]cp.BindCredential{"sys-a": cred}}
	_, addr := startTestListener(t, store, &trackedRegistry{}, Options{Now: func() time.Time { return graceNow }})

	// Bind with the CURRENT secret: no grace cutoff is armed.
	c := dialBind(t, addr, "sys-a", bindPW)

	// Wait past the grace deadline by exercising the live session; it must still answer.
	deadlineWait := time.After(120 * time.Millisecond)
	<-deadlineWait
	expectAlive(t, c, 2)

}
