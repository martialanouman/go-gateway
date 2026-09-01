package smppserver

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestBindThrottleFailsOpenWhenRedisIsCut is the step-250d acceptance test for the fifth row of the
// failure-policy matrix (guide de codage §16): "Redis (anti-brute-force de bind) -> fail-open : le
// throttle s'efface, argon2id reste seul juge".
//
// What stood in for it was fakeThrottle (throttle_test.go:24), whose Check returns a scripted error —
// and whose RecordFailure and Reset return nil unconditionally, so their own fail-open branches
// (listener.go:283, :294) have never run at all. It also cannot produce the state that makes this
// policy meaningful: a subject that is ACTUALLY blocked. "The bind succeeded during an outage" proves
// nothing on its own, because it succeeds against a healthy Redis too. Only a real counter, really
// crossed and really unreachable, separates fail-open from a throttle that was never going to block.
//
// It drives onBind directly rather than a socket: the policy is a DECISION (the throttle steps aside,
// argon2id stays the judge), not a wire format — unlike the session registry, whose whole question is
// which command_status reaches the ESME.
func TestBindThrottleFailsOpenWhenRedisIsCut(t *testing.T) {
	rdb, proxy := redistest.Cuttable(t)
	cfg := bindthrottle.Config{
		MaxFailures: 2,
		Window:      time.Minute,
		BackoffBase: time.Millisecond,
		BackoffMax:  time.Millisecond,
	}
	l := New(&fakeStore{cred: activeCred(t), found: true}, &fakeRegistry{accepted: true}, nil,
		Options{Throttle: bindthrottle.New(rdb, cfg)}, discardLog())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Unique subjects: the Redis container is shared across this package's tests, and both counters are
	// keyed by attacker-supplied strings.
	systemID := "sid-" + uuid.NewString()[:8]
	clientIP := "10.0.0." + uuid.NewString()[:2]

	bind := func(password string) uint32 {
		t.Helper()
		res := l.onBind(ctx, &connState{bindID: uuid.NewString()}, clientIP, nil)(
			ctx, session.BindRequest{SystemID: systemID, Password: password, Mode: session.BindTransceiver})
		return res.Status
	}

	// Control, with Redis up: drive the subject over MaxFailures and watch it get blocked. The password
	// is the CORRECT one, which is what makes the observation possible at all — a blocked bind answers
	// ESME_RINVPASWD, deliberately indistinguishable from a wrong password (listener.go:178-181), so a
	// valid credential is the only way to tell "refused by the throttle" from "refused by argon2id".
	for i := 0; i < cfg.MaxFailures; i++ {
		if err := l.opts.Throttle.RecordFailure(ctx, systemID, clientIP); err != nil {
			t.Fatalf("record failure %d: %v", i, err)
		}
	}
	if got := bind(testPassword); got != errs.StatusInvalidPasswd {
		t.Fatalf("with redis up a throttled bind status = %#x, want %#x: the control never blocked, so "+
			"the outage half below would prove nothing", got, errs.StatusInvalidPasswd)
	}

	proxy.Cut()

	// The outage: the same subject, still over its threshold, now reaches authentication and binds. A
	// throttle is defence in depth, not the authentication itself; a Redis fault must not close the SMPP
	// ingress to every legitimate ESME.
	if got := bind(testPassword); got != smpp.StatusOK {
		t.Errorf("with redis cut the bind status = %#x, want ESME_ROK: the anti-brute-force counter is "+
			"unreachable, and refusing on that basis would take the whole SMPP ingress down with Redis",
			got)
	}

	// And the half without which "fail-open" would be indistinguishable from "throttling disabled":
	// argon2id is still the judge. A wrong password is refused during the outage exactly as it would be
	// otherwise — the throttle stepped aside, authentication did not.
	if got := bind("wrong-" + testPassword); got != errs.StatusInvalidPasswd {
		t.Errorf("with redis cut a WRONG password got status = %#x, want %#x: failing open on the "+
			"throttle must never fail open on authentication", got, errs.StatusInvalidPasswd)
	}

	proxy.Resume()

	// Recovery: the lockout is still standing, and the reason is worth naming precisely rather than
	// plausibly. It is NOT that resetThrottle failed against the cut link — even a Reset that had
	// succeeded would not have lifted this block. Reset clears the system_id counter only
	// (throttle.go:143): the IP counter is deliberately left to decay on its own window, so one
	// legitimate bind cannot hand a clean slate to an attacker sharing a NAT. Check blocks on
	// max(system_id, ip), so the IP side outlives any reset. What the outage could not touch is that
	// both counters live on the SERVER — the proxy severed a link, not a database. A fail-open that
	// latched, or counters that evaporated with the outage, would make a Redis blip a way to clear
	// one's record.
	if got := bind(testPassword); got != errs.StatusInvalidPasswd {
		t.Errorf("after redis came back the throttled subject bound with status %#x, want %#x: the "+
			"lockout did not survive the outage, so a Redis blip is a way to clear one's record", got,
			errs.StatusInvalidPasswd)
	}
}
