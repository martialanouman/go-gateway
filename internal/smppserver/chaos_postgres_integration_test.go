package smppserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	"github.com/martialanouman/go-gateway/internal/smppserver"

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
	seed := pgtest.Pool(t)     // uncut: seeds the credential, and is the witness that the cut took only one link
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

	// The cut severs THIS pool's link and nothing else — asserted, not assumed. A harness that took the
	// shared container down instead would make every fail-closed assertion below pass for a reason that
	// has nothing to do with the policy, and would wreck the sibling tests besides.
	if err := seed.Ping(context.Background()); err != nil {
		t.Fatalf("the uncut pool died with the cut one (%v): tcpproxy must sever one client, never the "+
			"container the whole package shares", err)
	}

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

// TestAPostgresOutageNeverFeedsTheBindThrottle guards the half of the SMPP-bind failure policy that
// the test above cannot see, and that a review of step-260c found the matrix about to state wrongly.
//
// authorize's ESME_RSYSERR is not the last word: onBind counts EVERY non-OK status as an
// authentication failure (listener.go:190) and the anti-brute-force throttle is consulted BEFORE
// authentication, refusing with ESME_RINVPASWD (listener.go:183). Production wires that throttle
// unconditionally (cmd/smpp-server-svc/wiring.go:219,276) with SMPP_BIND_MAX_FAILURES defaulting to 5.
// So without this guard, the sixth bind of an outage answers "your password is wrong" — the exact
// signal the policy exists to avoid — and the sliding window holds the lockout open for as long as the
// outage lasts, then past it.
//
// The spec settles which failures may count: §6.3 says "échecs d'auth comptés par system_id et IP" and
// step-026 repeats it. A database that cannot answer is an infrastructure fault, not a failed
// authentication, and it is not attacker-inducible either — nothing a client sends can make the
// credential lookup error — so declining to count it costs the throttle no signal it could use.
func TestAPostgresOutageNeverFeedsTheBindThrottle(t *testing.T) {
	seed := pgtest.Pool(t)
	rdb := redistest.Client(t)
	registry := startRegistry(t, rdb)

	// A low threshold keeps the test short; the backoff is what a blocked bind waits before being
	// refused, so it stays small for the same reason. Neither is the behaviour under test.
	const maxFailures = 3
	throttle := bindthrottle.New(rdb, bindthrottle.Config{
		MaxFailures: maxFailures,
		Window:      time.Minute,
		BackoffBase: 10 * time.Millisecond,
		BackoffMax:  10 * time.Millisecond,
	})

	// The IP counter is keyed by source address, and every ESME in this package dials from 127.0.0.1
	// against the SHARED Redis. A sibling test's failures would trip the threshold before ours and make
	// this test pass for the wrong reason; the system_id counter needs no such care, seedBind minting a
	// fresh one per run.
	if err := rdb.Del(context.Background(), "bindfail:ip:{127.0.0.1}").Err(); err != nil {
		t.Fatalf("clear the shared ip failure counter: %v", err)
	}

	cutPool, proxy := pgtest.Cuttable(t)
	addr := startListener(t, cutPool, registry, func(o *smppserver.Options) { o.Throttle = throttle })

	sid, pw, _ := seedBind(t, seed, seedOpts{maxSessions: 3, bindType: cp.BindTRX})

	// Control, link up: the bind is accepted, which also proves the throttle is not blocking anything
	// before the outage even starts.
	accepted := dialESME(t, addr)
	if got := accepted.bind(t, smppsession.BindTransceiver, sid, pw); got != smpp.StatusOK {
		t.Fatalf("with postgres up the bind status = %#x, want ESME_ROK — the control failed", got)
	}
	accepted.unbind(t)
	accepted.close()

	proxy.Cut()

	// One more attempt than the threshold: if the outage were feeding the counter, the last one would
	// be refused by the throttle instead of by authorize.
	for attempt := 1; attempt <= maxFailures+1; attempt++ {
		e := dialESME(t, addr)
		got := e.bind(t, smppsession.BindTransceiver, sid, pw)
		e.close()
		if got == errs.StatusInvalidPasswd {
			t.Fatalf("outage bind %d of %d answered %#x (ESME_RINVPASWD): the outage has been counted as "+
				"an authentication failure and the anti-brute-force throttle has locked the account out. "+
				"The ESME now reads \"your secret is wrong\" — the one signal this policy exists to avoid — "+
				"and the window slides on every further attempt, so the lockout outlives the outage",
				attempt, maxFailures+1, got)
		}
		if got != errs.StatusSysErr {
			t.Fatalf("outage bind %d status = %#x, want %#x (ESME_RSYSERR)", attempt, got, errs.StatusSysErr)
		}
	}

	proxy.Resume()

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
			t.Fatalf("after postgres came back the bind status = %#x, want ESME_ROK: a lockout earned "+
				"during the outage is still refusing binds the database can now answer", status)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
