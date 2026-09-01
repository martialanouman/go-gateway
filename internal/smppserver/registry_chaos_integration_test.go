package smppserver_test

import (
	"testing"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	smppsession "github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestBindFailsClosedWhenTheSessionRegistryIsCut is the step-250d acceptance test for the fourth row of
// the failure-policy matrix (guide de codage §16): "Redis (registre de sessions) -> fail-closed : le
// bind est refusé (ESME_RSYSERR)".
//
// Of the four rows step-250 wrote and never tested, this is the only one with NO coverage of any kind.
// The fiche cited server_test.go:121 as "a fake that errs", but that test is a fake PUBLISHER on the
// Disconnect path, built with session.NewServer(nil, pub) — a nil registry. Nothing has ever exercised
// Registry.Bind under a Redis fault, and nothing could with a fake: Registry holds a concrete
// *redis.Client (registry.go:54), not an interface. Only a real cut reaches it.
//
// It runs end to end on purpose. The refusal is assembled across a gRPC hop: the registry answers
// codes.Internal with NO ErrorInfo detail, and registryBindStatus (bind.go:135-153) iterates an empty
// detail list, fails to match ResourceExhausted, and falls through to its final `return
// errs.StatusSysErr`. The policy is therefore correct by a FALLBACK BRANCH — the one place a change
// would go unnoticed — and only the wire tells you which of the two camps the ESME was put in.
func TestBindFailsClosedWhenTheSessionRegistryIsCut(t *testing.T) {
	pool := pgtest.Pool(t)
	rdb, proxy := redistest.Cuttable(t)
	registry := startRegistry(t, rdb)
	addr := startListener(t, pool, registry)

	// Two accounts, because one cannot tell the two camps apart. The quota account proves what a
	// REFUSAL ON THE MERITS looks like; the roomy one is where the outage is measured, with a free slot
	// so that any refusal there can only be the fault.
	quotaSID, quotaPW, _ := seedBind(t, pool, seedOpts{maxSessions: 1, bindType: cp.BindTRX})
	roomySID, roomyPW, _ := seedBind(t, pool, seedOpts{maxSessions: 2, bindType: cp.BindTRX})

	// Control A, with Redis up: a bind within quota is accepted. Without it the outage half could not
	// distinguish fail-closed from a listener that refuses everything.
	held := dialESME(t, addr)
	defer held.close()
	if got := held.bind(t, smppsession.BindTransceiver, roomySID, roomyPW); got != smpp.StatusOK {
		t.Fatalf("with redis up the bind status = %#x, want ESME_ROK", got)
	}

	// Control B, with Redis up: a bind BEYOND quota is refused ESME_RBINDFAIL. This is the camp the
	// outage must NOT be confused with — and the confusion would be expensive rather than cosmetic.
	// ESME_RBINDFAIL is catalogued Retryable (errors.go:150), so an ESME that reads it reconnects at
	// once; a whole fleet doing that against a registry that is already down is a stampede on the way
	// back up.
	filler := dialESME(t, addr)
	if got := filler.bind(t, smppsession.BindTransceiver, quotaSID, quotaPW); got != smpp.StatusOK {
		t.Fatalf("filling the quota account: status = %#x, want ESME_ROK", got)
	}
	over := dialESME(t, addr)
	if got := over.bind(t, smppsession.BindTransceiver, quotaSID, quotaPW); got != errs.StatusBindFail {
		t.Fatalf("over-quota bind status = %#x, want %#x (ESME_RBINDFAIL)", got, errs.StatusBindFail)
	}
	over.close()
	filler.unbind(t)
	filler.close()

	proxy.Cut()

	// The outage. The roomy account still has a free slot, so a refusal here is the registry being
	// unreachable and nothing else.
	during := dialESME(t, addr)
	got := during.bind(t, smppsession.BindTransceiver, roomySID, roomyPW)
	during.close()
	if got == smpp.StatusOK {
		t.Fatal("with the session registry cut the bind was ACCEPTED: max_sessions (invariant d) is not " +
			"opposable without the registry, so admitting the bind admits an unbounded number of them")
	}
	if got != errs.StatusSysErr {
		t.Errorf("outage bind status = %#x, want %#x (ESME_RSYSERR): %#x is ESME_RBINDFAIL, which tells "+
			"the ESME its quota is full and to come straight back — the wrong instruction to give a fleet "+
			"while the registry is down", got, errs.StatusSysErr, errs.StatusBindFail)
	}

	proxy.Resume()

	// Recovery, and the part a refusal alone would not establish: the aborted attempt consumed nothing.
	// bind.lua is atomic, so a severed link must leave no half-written member holding a slot — the
	// roomy account still has room for exactly the bind the outage denied.
	after := dialESME(t, addr)
	defer after.close()
	if got := after.bind(t, smppsession.BindTransceiver, roomySID, roomyPW); got != smpp.StatusOK {
		t.Fatalf("after redis came back the retried bind status = %#x, want ESME_ROK: the attempt refused "+
			"during the outage left a phantom member holding a slot", got)
	}
}
