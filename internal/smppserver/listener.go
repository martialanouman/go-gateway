package smppserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
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

		l.wg.Add(1)
		go l.serve(ctx, nc)
	}
}

// connState carries a connection's bind identity between the OnBind hook (which populates it on a
// successful bind) and the post-Serve cleanup (which releases the token). Both run on the connection's
// own goroutine in sequence, so the fields need no lock; the refresh goroutine only reads fields set
// before it starts and never mutated after.
type connState struct {
	accountID   string
	bindID      string
	maxSessions int32
	bound       bool

	refreshStop context.CancelFunc
	refreshDone chan struct{}
}

// serve drives one accepted connection through an authenticated SMPP session, then releases its
// registry token. The token is freed uniformly whatever ended the session — graceful unbind, peer EOF,
// idle drop or pod drain — because it runs after Serve returns regardless of the reason.
func (l *Listener) serve(ctx context.Context, nc net.Conn) {
	defer l.wg.Done()
	defer func() { _ = nc.Close() }()

	st := &connState{bindID: uuid.NewString()}

	sess := session.New(nc, session.Config{
		SystemID:    l.opts.SystemID,
		IdleTimeout: l.opts.IdleTimeout,
		Logger:      l.logger,
		OnBind:      l.onBind(ctx, st),
		OnSubmit:    rejectSubmit,
	})
	_ = sess.Serve(ctx)

	// Stop the refresh loop (if any) before releasing, so a refresh cannot race the Unbind.
	if st.refreshStop != nil {
		st.refreshStop()
		<-st.refreshDone
	}
	if st.bound {
		l.releaseToken(ctx, st)
	}
}

// onBind returns the session's bind decision hook, bound to this connection's state. On success it
// reserves a session token against the registry (invariant d) and starts the refresh loop that keeps it
// alive; on any rejection it returns the mapped command_status without touching the registry.
func (l *Listener) onBind(ctx context.Context, st *connState) session.BindHandler {
	return func(bctx context.Context, req session.BindRequest) session.BindResult {
		cred, cmdStatus := l.authorize(bctx, req)
		if cmdStatus != smpp.StatusOK {
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

		st.accountID = cred.AccountID.String()
		st.maxSessions = cred.MaxSessions
		st.bound = true
		l.startRefresh(ctx, st, req.Mode)

		l.logger.InfoContext(bctx, "smpp bind accepted",
			"account_id", cred.AccountID, "mode", req.Mode, "active_sessions", resp.GetActiveSessions())
		return session.BindResult{Status: smpp.StatusOK}
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
					AccountId: st.accountID,
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
		AccountId: st.accountID,
		BindId:    st.bindID,
	}); err != nil {
		// The token will lapse on its own TTL, so this is a warning, not a fault. errors.Is guards
		// against a nil-typed error slipping through in tests.
		if !errors.Is(err, context.Canceled) {
			l.logger.Warn("smpp session token release failed", "err", err, "account_id", st.accountID)
		}
	}
}
