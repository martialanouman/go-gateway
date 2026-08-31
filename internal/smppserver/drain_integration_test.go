package smppserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
	smppsession "github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/smppserver"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// startDrainableListener starts a listener under a pod identity the caller chooses and hands back the
// cancel that drains it. The pod ID is a parameter, not the shared "pod-test", because the registry
// keys a session as "pod_id:bind_id": a pod that restarts comes back under a NEW name, so its own
// leftover entry is a DIFFERENT member and bind.lua's "a rebind does not double-count" rule does not
// cover it. A test that reused one pod ID would prove the easy case and miss the deployed one.
func startDrainableListener(t *testing.T, pool *pgxpool.Pool, registry registrypb.SessionRegistryClient, podID string) (string, context.CancelFunc) {
	t.Helper()
	l := smppserver.New(postgres.NewBindRepo(pool), registry, nil, smppserver.Options{
		Addr:     "127.0.0.1:0",
		PodID:    podID,
		SystemID: "smpp-server-svc",
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = l.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-stopped })

	addrCtx, addrCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer addrCancel()
	addr, err := l.Addr(addrCtx)
	if err != nil {
		t.Fatalf("listener addr: %v", err)
	}
	return addr, cancel
}

// TestDrainUnbindsAndFreesTheSlotForTheNextPod is the "binds préservés" criterion of plan §16, at the
// only layer where it is true or false: the real listener, the real registry, a real quota of one.
//
// It asserts the two halves an ESME actually experiences across a rolling deploy.
//
//   - The draining pod takes its leave. An ESME that receives an unbind knows the peer went away on
//     purpose and reconnects at once; one that sees a bare FIN treats it as a network fault and waits
//     out its error backoff first. This is the guide de codage §5 [MUST], end to end rather than at the
//     session unit.
//   - The replacement pod can be bound immediately. The registry entry is "pod_id:bind_id" and the new
//     pod has a new ID, so nothing merges the old member into the new bind: if the drain did not
//     release the token, the slot stays taken until DefaultSessionTTL (60s) sweeps it, and the ESME is
//     refused ESME_RBINDFAIL by the very quota meant to protect it. max_sessions is 1 so that refusal
//     is certain rather than probabilistic.
func TestDrainUnbindsAndFreesTheSlotForTheNextPod(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)

	sid, pw, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})

	addrA, drainA := startDrainableListener(t, pool, registry, "pod-a")

	client := dialESME(t, addrA)
	defer client.close()
	if got := client.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("bind on pod-a status = %#x, want ESME_ROK — the control failed", got)
	}

	drainA()

	_ = client.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	pdu, err := smpp.ReadPDU(client.conn)
	if err != nil {
		t.Fatalf("draining pod sent no PDU (%v): the ESME saw a bare socket close, not an unbind — "+
			"a rolling deploy is then indistinguishable from a network fault", err)
	}
	if pdu.CommandID() != smpp.CmdUnbind {
		t.Fatalf("draining pod sent cmd = %#x, want unbind", pdu.CommandID())
	}

	// The replacement pod, under a new identity as Kubernetes would roll it.
	addrB, _ := startDrainableListener(t, pool, registry, "pod-b")

	// The token release runs after the session goroutine returns, so allow a brief settle — but the
	// budget is 2s against a 60s TTL: passing by TTL expiry is not on the table.
	if got := eventuallyBind(t, addrB, sid, pw); got != smpp.StatusOK {
		t.Fatalf("bind on pod-b status = %#x, want ESME_ROK: the drained pod never released its "+
			"session token, so max_sessions still counts a bind nobody holds", got)
	}
}
