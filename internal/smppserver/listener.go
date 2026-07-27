package smppserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// Run listens on Options.Addr and serves each accepted connection until ctx is cancelled, then drains
// the connections already in flight. It is a supervisor component: a genuine listen/accept fault brings
// the service down, while a shutdown-triggered accept error is an orderly stop (returns nil). Run
// blocks; the supervisor runs it on its own goroutine.
func (l *Listener) Run(ctx context.Context) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", l.opts.Addr)
	if err != nil {
		return fmt.Errorf("smppserver: listen on %q: %w", l.opts.Addr, err)
	}

	// Closing the listener when ctx ends unblocks Accept; the connections drain on the same ctx, which
	// their sessions observe as an orderly close.
	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()

	// Publish the resolved address (an ephemeral ":0" bind only learns its port now) so Addr can report it.
	l.mu.Lock()
	l.addr = lis.Addr().String()
	l.mu.Unlock()
	close(l.ready)

	l.logger.InfoContext(ctx, "smpp listener listening", "addr", lis.Addr().String())
	// slots caps concurrent connections (Options.MaxConns): max_sessions is only enforced after a
	// successful bind, so without this an unauthenticated flood — especially of tarpitted binds held by
	// the throttle backoff — could pin an unbounded number of goroutines and file descriptors. Acquiring
	// before serving applies backpressure: when full, Accept stops and new connections queue in the
	// kernel backlog until a slot frees.
	slots := make(chan struct{}, l.opts.MaxConns)
	for {
		nc, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				// The listener was closed on shutdown: wait for accepted connections to finish, then stop.
				l.wg.Wait()
				return nil
			}
			return fmt.Errorf("smppserver: accept: %w", err)
		}

		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			_ = nc.Close()
			l.wg.Wait()
			return nil
		}
		l.wg.Add(1)
		go func() {
			defer func() { <-slots }()
			l.serve(ctx, nc)
		}()
	}
}

// connState carries a connection's bind identity between the OnBind hook (which populates it on a
// successful bind) and the post-Serve cleanup (which releases the token). Both run on the connection's
// own goroutine in sequence, so the fields need no lock; the refresh goroutine only reads fields set
// before it starts and never mutated after.
type connState struct {
	accountID       uuid.UUID
	customerID      uuid.UUID
	bindID          string
	mode            session.BindMode
	maxSessions     int32
	querySMEnabled  bool
	cancelSMEnabled bool
	bound           bool

	refreshStop context.CancelFunc
	refreshDone chan struct{}

	// graceStop/graceDone control the pod-local cutoff armed for a bind authenticated with the grace
	// (previous) secret: it force-closes the session when the rotation grace window ends (step-032).
	// nil unless this bind authenticated via grace.
	graceStop context.CancelFunc
	graceDone chan struct{}
}

// serve drives one accepted connection through an authenticated SMPP session, then releases its
// registry token. The token is freed uniformly whatever ended the session — graceful unbind, peer EOF,
// idle drop or pod drain — because it runs after Serve returns regardless of the reason.
func (l *Listener) serve(ctx context.Context, nc net.Conn) {
	defer l.wg.Done()
	defer func() { _ = nc.Close() }()

	st := &connState{bindID: uuid.NewString()}
	clientIP := remoteIP(nc)

	// onBound runs on a successful bind, inside Serve. It records the live session in the pod-local
	// registry (so a force-disconnect can reach this socket) and, for a grace-authenticated bind, arms
	// the cutoff that closes it when the rotation grace window ends. sess is captured by reference: it
	// is assigned just below, before Serve — and thus before onBound can ever run.
	var sess *session.Session
	onBound := func(viaGrace bool, graceExpiresAt *time.Time) {
		l.registerSession(st, sess)
		if viaGrace && graceExpiresAt != nil {
			l.startGraceDeadline(ctx, st, sess, *graceExpiresAt)
		}
	}

	sess = session.New(nc, session.Config{
		SystemID:      l.opts.SystemID,
		IdleTimeout:   l.opts.IdleTimeout,
		InboundWindow: l.opts.InboundWindow,
		Logger:        l.logger,
		OnBind:        l.onBind(ctx, st, clientIP, onBound),
		OnSubmit:      l.onSubmit(ctx, st),
		OnQuery:       l.onQuery(ctx, st),
		OnCancel:      l.onCancel(ctx, st),
	})
	_ = sess.Serve(ctx)

	// Stop the refresh loop (if any) before releasing, so a refresh cannot race the Unbind. Stop the
	// grace cutoff too, so a session that ended before its grace deadline leaks no timer goroutine.
	if st.refreshStop != nil {
		st.refreshStop()
		<-st.refreshDone
	}
	if st.graceStop != nil {
		st.graceStop()
		<-st.graceDone
	}
	if st.bound {
		l.deregisterSession(st.bindID)
		l.releaseToken(ctx, st)
	}
}

// onBind returns the session's bind decision hook, bound to this connection's state and source IP. It
// consults the anti-brute-force throttle before authentication (refusing without paying the argon2id
// cost past the threshold); on success it reserves a session token against the registry (invariant d),
// starts the refresh loop that keeps it alive and clears the throttle counter; on any rejection it
// returns the mapped command_status without touching the registry.
func (l *Listener) onBind(ctx context.Context, st *connState, clientIP string, onBound func(viaGrace bool, graceExpiresAt *time.Time)) session.BindHandler {
	return func(bctx context.Context, req session.BindRequest) session.BindResult {
		// A throttled subject is refused before authentication, so a brute-force flood cannot make the
		// server pay the argon2id cost. The refusal answers ESME_RINVPASWD — indistinguishable from a
		// wrong password, so an attacker cannot detect the lockout (consistent with authorize's
		// anti-enumeration posture). This deliberately departs from step-026's literal ESME_RBINDFAIL.
		if l.throttleBlocks(bctx, req.SystemID, clientIP) {
			return session.BindResult{Status: errs.StatusInvalidPasswd}
		}

		cred, cmdStatus, viaGrace := l.authorize(bctx, req)
		if cmdStatus != smpp.StatusOK {
			// An authentication or authorisation failure feeds the throttle; a registry quota rejection
			// (below) does not, since valid credentials over max_sessions are no brute-force signal.
			l.recordBindFailure(bctx, req.SystemID, clientIP)
			l.logger.InfoContext(bctx, "smpp bind rejected", "mode", req.Mode, "command_status", cmdStatus)
			return session.BindResult{Status: cmdStatus}
		}

		sess := &registrypb.Session{
			AccountId: cred.AccountID.String(),
			SystemId:  req.SystemID,
			PodId:     l.opts.PodID,
			BindId:    st.bindID,
			BindType:  pbBindType(req.Mode),
		}
		resp, err := l.registry.Bind(bctx, &registrypb.BindRequest{Session: sess, MaxSessions: cred.MaxSessions})
		if err != nil {
			cmdStatus := registryBindStatus(err)
			l.logger.InfoContext(bctx, "smpp bind refused by registry",
				"account_id", cred.AccountID, "mode", req.Mode, "command_status", cmdStatus)
			return session.BindResult{Status: cmdStatus}
		}
		if !resp.GetAccepted() {
			// The server rejects a quota breach with an error, not accepted=false; guard defensively so a
			// contract drift can never over-admit past max_sessions.
			return session.BindResult{Status: errs.StatusBindFail}
		}

		st.accountID = cred.AccountID
		st.customerID = cred.CustomerID
		st.maxSessions = cred.MaxSessions
		st.querySMEnabled = cred.QuerySMEnabled
		st.cancelSMEnabled = cred.CancelSMEnabled
		st.mode = req.Mode
		st.bound = true
		l.startRefresh(ctx, st, req.Mode)

		// Publish the live session (and arm the grace cutoff if this bind used the previous secret) only
		// now that st carries the account/customer identity a scoped Disconnect matches on.
		if onBound != nil {
			onBound(viaGrace, cred.GraceExpiresAt)
		}

		// A successful bind clears the system_id's failure counter (the IP counter is left to decay on
		// its own window, so one success cannot launder an attacker sharing the source IP).
		l.resetThrottle(bctx, req.SystemID)

		l.logger.InfoContext(bctx, "smpp bind accepted",
			"account_id", cred.AccountID, "mode", req.Mode, "active_sessions", resp.GetActiveSessions())
		return session.BindResult{Status: smpp.StatusOK}
	}
}

// throttleBlocks reports whether the anti-brute-force throttle refuses this bind, and when it does,
// applies the backoff, counts the block and emits the auditable security event. It is fail-open: a nil
// throttle (throttling disabled) or a Redis error lets the bind proceed to authentication, so a Redis
// outage never takes down the SMPP ingress.
func (l *Listener) throttleBlocks(ctx context.Context, systemID, clientIP string) bool {
	if l.opts.Throttle == nil {
		return false
	}
	dec, err := l.opts.Throttle.Check(ctx, systemID, clientIP)
	if err != nil {
		l.logger.WarnContext(ctx, "smpp bind throttle check failed; failing open", "err", err)
		return false
	}
	if !dec.Blocked {
		return false
	}

	// Apply the backoff and refuse — but do NOT re-count here. The failure that crossed the threshold
	// already counted; incrementing on every blocked attempt would let an attacker who varies the
	// (attacker-controlled) system_id mint unbounded Redis keys, and would hold a shared IP's lockout
	// open forever. The block instead lifts on its own sliding window once the attacker relents.
	sleep(ctx, dec.RetryAfter)
	if l.opts.ThrottleBlocked != nil {
		l.opts.ThrottleBlocked.WithLabelValues(dec.Subject).Inc()
	}
	// Auditable security event. The system_id and the source IP are identifiers, not secrets, so they
	// are logged deliberately here (never a password or a body); the wire response stays RINVPASWD.
	l.logger.WarnContext(ctx, "smpp bind throttled",
		"system_id", systemID, "client_ip", clientIP, "failures", dec.Failures, "backoff", dec.RetryAfter)
	return true
}

// recordBindFailure counts an unsuccessful bind against the throttle. A nil throttle is a no-op; a
// Redis error is logged and swallowed (fail-open), never failing the bind path over it.
func (l *Listener) recordBindFailure(ctx context.Context, systemID, clientIP string) {
	if l.opts.Throttle == nil {
		return
	}
	if err := l.opts.Throttle.RecordFailure(ctx, systemID, clientIP); err != nil {
		l.logger.WarnContext(ctx, "smpp bind throttle record failed", "err", err)
	}
}

// resetThrottle clears a system_id's failure counter after a successful bind. A nil throttle is a
// no-op; a Redis error is logged and swallowed.
func (l *Listener) resetThrottle(ctx context.Context, systemID string) {
	if l.opts.Throttle == nil {
		return
	}
	if err := l.opts.Throttle.Reset(ctx, systemID); err != nil {
		l.logger.WarnContext(ctx, "smpp bind throttle reset failed", "err", err)
	}
}

// sleep waits for d or until ctx is cancelled, whichever comes first. The throttle backoff must not
// pin a goroutine past a pod drain, so the tarpit delay always observes cancellation.
func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// remoteIP extracts the connection's source IP (without the ephemeral port, which would make a
// throttle key useless and unbounded). It falls back to the full remote address if it is not host:port.
//
// OPERATIONAL: this is the transport peer address. Behind an L4/TCP load balancer without PROXY
// protocol it is the balancer's address, not the ESME's — collapsing every client onto one
// bindfail:ip key, i.e. a global throttle (self-DoS). Deploy with L7 termination or PROXY protocol so
// the peer is the real client, or neutralise the per-IP counter for that topology. See
// tasks-done/step-026.md (deployment topology undecided as of this change).
func remoteIP(nc net.Conn) string {
	host, _, err := net.SplitHostPort(nc.RemoteAddr().String())
	if err != nil {
		return nc.RemoteAddr().String()
	}
	return host
}

// liveSession is one bind this pod owns, held so a force-disconnect can reach its socket. account and
// customer ids select the session on a scoped Disconnect; sess is the closable SMPP session. mode is
// the bind role, read (immutably) by Deliver to refuse a transmitter, which cannot receive deliver_sm.
type liveSession struct {
	accountID  uuid.UUID
	customerID uuid.UUID
	bindID     string
	mode       session.BindMode
	sess       *session.Session
}

// registerSession records a freshly bound session in the pod-local registry, keyed by bind_id, so a
// later Disconnect order can reach its socket. It reads the account/customer identity from st, which
// onBind has already populated.
func (l *Listener) registerSession(st *connState, sess *session.Session) {
	l.sessMu.Lock()
	defer l.sessMu.Unlock()
	l.sessions[st.bindID] = &liveSession{
		accountID:  st.accountID,
		customerID: st.customerID,
		bindID:     st.bindID,
		mode:       st.mode,
		sess:       sess,
	}
}

// deregisterSession drops a session from the pod-local registry once its connection has ended, so the
// map never holds a dead bind. It is idempotent: a bind_id already gone is a no-op.
func (l *Listener) deregisterSession(bindID string) {
	l.sessMu.Lock()
	delete(l.sessions, bindID)
	l.sessMu.Unlock()
}

// Disconnect force-closes every live session this pod owns for the scoped account or customer,
// answering a fan-out order (step-032) triggered by a revocation or a suspension. It snapshots the
// matching sessions under the lock, then closes them outside it, so a slow socket write can never
// stall the disconnect fan-out or block a concurrent bind. It is bounded by the pod's own live-session
// count and idempotent: a session already gone is closed again harmlessly, and a repeated order is a
// no-op. Identifiers and the reason are logged; a secret or a body never is (§1.9).
func (l *Listener) Disconnect(scope disconnect.Scope, id, reason string) {
	l.sessMu.Lock()
	targets := make([]*liveSession, 0)
	for _, ls := range l.sessions {
		if scopeMatches(ls, scope, id) {
			targets = append(targets, ls)
		}
	}
	l.sessMu.Unlock()

	for _, ls := range targets {
		l.forceClose(ls, reason)
	}
}

// scopeMatches reports whether ls belongs to the scoped account or customer. An unknown scope matches
// nothing (Decode already rejects one, so this is defence in depth).
func scopeMatches(ls *liveSession, scope disconnect.Scope, id string) bool {
	switch scope {
	case disconnect.ScopeAccount:
		return ls.accountID.String() == id
	case disconnect.ScopeCustomer:
		return ls.customerID.String() == id
	default:
		return false
	}
}

// forceClose terminates one live session and records why. The socket close makes Serve return, which
// runs the connection's own cleanup (deregisterSession + releaseToken), so the max_sessions slot is
// freed on the pod's normal path. Close is idempotent, so racing another force-close or the session's
// own teardown is safe.
func (l *Listener) forceClose(ls *liveSession, reason string) {
	l.logger.Info("smpp session force-disconnected",
		"account_id", ls.accountID, "bind_id", ls.bindID, "reason", reason)
	ls.sess.Close()
}

// startGraceDeadline launches the pod-local cutoff for a bind authenticated with the previous
// (grace) secret: it closes the session when the rotation grace window ends. It reads deadline as
// given (step-027 wrote it; the cutoff never recomputes it) and is cancellable via st.graceStop,
// joined through st.graceDone — so a session that unbinds before the deadline leaks no goroutine.
func (l *Listener) startGraceDeadline(ctx context.Context, st *connState, sess *session.Session, deadline time.Time) {
	gctx, cancel := context.WithCancel(ctx)
	st.graceStop = cancel
	st.graceDone = make(chan struct{})
	go l.graceDeadlineLoop(gctx, st, sess, deadline, st.graceDone)
}

// graceDeadlineLoop force-closes the session at deadline, or exits early if the session ends first
// (ctx cancelled). The delay is measured against the injectable clock, so a test advances time
// without sleeping; a deadline already in the past fires immediately.
func (l *Listener) graceDeadlineLoop(ctx context.Context, st *connState, sess *session.Session, deadline time.Time, done chan struct{}) {
	defer close(done)

	d := deadline.Sub(l.opts.Now())
	if d < 0 {
		d = 0
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		l.forceClose(&liveSession{accountID: st.accountID, bindID: st.bindID, sess: sess}, "grace_expired")
	}
}

// startRefresh launches the per-session token refresh loop, cancellable via st.refreshStop and joined
// through st.refreshDone. The loop's context derives from the connection's, so a pod drain stops it too.
func (l *Listener) startRefresh(ctx context.Context, st *connState, mode session.BindMode) {
	rctx, cancel := context.WithCancel(ctx)
	st.refreshStop = cancel
	st.refreshDone = make(chan struct{})
	go l.refreshLoop(rctx, st, mode, st.refreshDone)
}

// refreshLoop re-Binds the session's member on a fixed interval to refresh its registry TTL, so the
// token never lapses under a live bind (a re-Bind of an existing member refreshes the TTL and never
// double-counts). It stops when its context is cancelled — by st.refreshStop after the session ends, or
// by a pod drain. A refresh failure is logged; the token will lapse on its own if the registry stays
// unreachable, which correctly frees the slot.
func (l *Listener) refreshLoop(ctx context.Context, st *connState, mode session.BindMode, done chan struct{}) {
	defer close(done)

	ticker := time.NewTicker(l.opts.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rctx, cancel := context.WithTimeout(ctx, registryCallTimeout)
			_, err := l.registry.Bind(rctx, &registrypb.BindRequest{
				Session: &registrypb.Session{
					AccountId: st.accountID.String(),
					PodId:     l.opts.PodID,
					BindId:    st.bindID,
					BindType:  pbBindType(mode),
				},
				MaxSessions: st.maxSessions,
			})
			cancel()
			if err != nil {
				l.logger.WarnContext(ctx, "smpp session token refresh failed",
					"err", err, "account_id", st.accountID)
			}
		}
	}
}

// releaseToken frees the session token after the connection ends. It detaches the connection's context
// with WithoutCancel so the Unbind still succeeds while the pod is draining (the connection's context is
// already cancelled by then), while keeping its trace context; a fresh timeout bounds the call.
func (l *Listener) releaseToken(parent context.Context, st *connState) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), registryCallTimeout)
	defer cancel()

	if _, err := l.registry.Unbind(ctx, &registrypb.UnbindRequest{
		AccountId: st.accountID.String(),
		BindId:    st.bindID,
	}); err != nil {
		// The token will lapse on its own TTL, so this is a warning, not a fault. errors.Is guards
		// against a nil-typed error slipping through in tests.
		if !errors.Is(err, context.Canceled) {
			l.logger.Warn("smpp session token release failed", "err", err, "account_id", st.accountID)
		}
	}
}
