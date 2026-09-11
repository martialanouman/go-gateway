package smppserver_test

import (
	"testing"
	"time"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	smppsession "github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestBindFailsClosedWhenPostgresIsCut is the step-260c acceptance test for the SMPP-bind row of the
// failure-policy matrix (guide de codage §16): a PostgreSQL outage refuses the bind with ESME_RSYSERR,
// and there is no cache to fall back on — every bind reads the control plane.
//
// The branch has one existing test, bind_test.go's fakeStore{err: errors.New("db down")}. That is the
// SHAPE of a fault, not its contract: a fake cannot show that nothing else in the path — a cache, a
// retry, a pod-local memory of the last credential — quietly admits the bind anyway. Only a real cut
// can, which is why §16 gets its row from this test and not from that one.
//
// Two controls run with the link up, and both are load-bearing. The accepted bind rules out a listener
// that refuses everything. The wrong-password bind names the camp the outage must NOT be confused
// with: ESME_RINVPASWD is what authorize returns for an unknown system_id or a bad secret
// (bind.go:40), so "the outage did not answer RINVPASWD" proves nothing until RINVPASWD has been seen
// for the reason it exists.
func TestBindFailsClosedWhenPostgresIsCut(t *testing.T) {
	seed := pgtest.Pool(t)     // uncut: seeds the credential, and survives the outage to prove nothing else did
	rdb := redistest.Client(t) // healthy on purpose: the only fault in this test must be Postgres
	registry := startRegistry(t, rdb)

	// Built while the link is still up — postgres.NewPool pings eagerly, so the cut comes after.
	cutPool, proxy := pgtest.Cuttable(t)
	addr := startListener(t, cutPool, registry)

	// Room for three: the controls below bind and unbind, and a quota refusal during the outage would
	// be indistinguishable from the fault it is meant to measure.
	sid, pw, _ := seedBind(t, seed, seedOpts{maxSessions: 3, bindType: cp.BindTRX})

	// Control A, link up: a valid bind is accepted.
	accepted := dialESME(t, addr)
	if got := accepted.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("with postgres up the bind status = %#x, want ESME_ROK — the control failed", got)
	}
	accepted.unbind(t)
	accepted.close()

	// Control B, link up: a wrong password answers ESME_RINVPASWD. This is the camp the outage must not
	// land in — an ESME that reads RINVPASWD concludes its credentials are wrong and stops retrying,
	// which is the opposite of what a transient database outage calls for.
	wrong := dialESME(t, addr)
	if got := wrong.bind(t, smppsession.BindTransceiver, sid, "wrongpw1"); got != errs.StatusInvalidPasswd {
		t.Fatalf("with postgres up a wrong password = %#x, want %#x (ESME_RINVPASWD) — without this "+
			"control, the outage assertion below cannot tell the two camps apart",
			got, errs.StatusInvalidPasswd)
	}
	wrong.close()

	proxy.Cut()

	// The outage, on credentials that are valid and a quota that has room: any refusal here is Postgres
	// being unreachable and nothing else.
	during := dialESME(t, addr)
	got := during.bind(t, smppsession.BindTransceiver, sid, pw)
	during.close()
	if got == smpp.StatusOK {
		t.Fatal("with postgres cut the bind was ACCEPTED: the bind path holds no cache and must not " +
			"acquire one — admitting an ESME whose credential, channel state and bind type could not " +
			"be read is admitting an unauthenticated session")
	}
	if got != errs.StatusSysErr {
		t.Errorf("outage bind status = %#x, want %#x (ESME_RSYSERR): %#x is ESME_RINVPASWD, which tells "+
			"the ESME its secret is wrong and to stop trying — a database outage is transient and the "+
			"ESME must come back", got, errs.StatusSysErr, errs.StatusInvalidPasswd)
	}

	// The same outage, on a system_id that does not exist. With the link up this is RINVPASWD by
	// design (§11.3 anti-enumeration); under the cut it must be RSYSERR, because the server cannot
	// know the account is unknown — it never reached the table. Getting RINVPASWD here would mean the
	// lookup error was being read as "no row", which silently turns an outage into an answer.
	unknown := dialESME(t, addr)
	gotUnknown := unknown.bind(t, smppsession.BindTransceiver, "no-such-esme", pw)
	unknown.close()
	if gotUnknown != errs.StatusSysErr {
		t.Errorf("outage bind on an unknown system_id = %#x, want %#x (ESME_RSYSERR): answering %#x "+
			"would assert the account does not exist, which a server that could not read the table "+
			"cannot know", gotUnknown, errs.StatusSysErr, errs.StatusInvalidPasswd)
	}

	proxy.Resume()

	// No latch, and the pool recovers on its own. The retry loop is the recovery of the infrastructure,
	// not the behaviour under test: pgx only discovers the connections killed by the cut by failing on
	// one, so the first bind back can still answer RSYSERR off a corpse.
	deadline := time.Now().Add(15 * time.Second)
	for {
		after := dialESME(t, addr)
		status := after.bind(t, smppsession.BindTransceiver, sid, pw)
		if status == smpp.StatusOK {
			after.unbind(t)
			after.close()
			break
		}
		after.close()
		if time.Now().After(deadline) {
			t.Fatalf("after postgres came back the bind status = %#x, want ESME_ROK: the listener "+
				"latched on the outage", status)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
