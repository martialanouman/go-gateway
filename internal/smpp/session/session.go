// Package session implements the server side of an SMPP v3.4 session over the internal/smpp codec:
// the state machine (open → bound → unbound), enquire_link handling and the outbound send window,
// with no business logic of its own. Authentication, max_sessions and routing are supplied by the
// caller through Config hooks (wired in step-024/step-025); the socket listener that accepts
// connections is step-024. A Session drives one already-established connection.
//
// Concurrency model (guide de codage §5/§9): Serve runs the sole read loop on the caller's
// goroutine and owns the state machine — state is never touched elsewhere. Socket writes (both the
// read loop's responses and a concurrent Send) are serialised by a write mutex, so a net.Conn is
// never written concurrently and a final unbind_resp is flushed before the connection closes.
// Every goroutine stops on the done channel, joined by Serve before it returns.
package session

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// defaultResponseTimeout bounds how long Send waits for the ESME to answer a server-initiated
// request (deliver_sm) when Config.ResponseTimeout is unset.
const defaultResponseTimeout = 10 * time.Second

// defaultInboundWindow is the per-session concurrent submit_sm ceiling when Config.InboundWindow is
// unset — high enough to keep a single bind's throughput off the produce-latency floor, bounded enough
// that one session cannot spawn unbounded workers.
const defaultInboundWindow = 100

// defaultInboundSubmitTimeout bounds a submit worker's produce when Config.InboundSubmitTimeout is
// unset: a generous backstop so a hung Kafka releases the worker (answering ESME_RSUBMITFAIL) rather
// than pinning it — and the shutdown drain — indefinitely.
const defaultInboundSubmitTimeout = 15 * time.Second

// unbindWriteTimeout bounds the best-effort outbound unbind sent by Close, so an unresponsive peer
// with a full send buffer can never pin the caller (a force-disconnect may close many sessions in a
// row). It only applies when the connection supports SetWriteDeadline.
const unbindWriteTimeout = 5 * time.Second

// ErrClosed is returned by Send when the session is shutting down.
var ErrClosed = errors.New("session: closed")

// Config configures a Session. The zero value is usable: it accepts every bind and answers every
// submit_sm with success. The hooks are where step-024/step-025 attach auth, max_sessions and the
// pipeline; the session itself decides neither.
type Config struct {
	// SystemID is the system_id returned in a bind_resp when a BindResult does not override it.
	SystemID string
	// WindowSize bounds how many server-initiated requests (deliver_sm, step-046) may be in flight
	// at once. Values below 1 are clamped to 1.
	WindowSize int
	// InboundWindow bounds how many submit_sm a session processes CONCURRENTLY (step-088): each is
	// dispatched to a worker so the read goroutine never blocks on its synchronous produce, and the
	// session's submits run in parallel up to this ceiling. A full window fails fast with ESME_RTHROTTLED
	// rather than blocking the read loop. Values below 1 are clamped to defaultInboundWindow.
	InboundWindow int
	// InboundSubmitTimeout bounds a submit worker's produce so a Kafka that hangs without a cancellation
	// cannot pin a worker (and thus the shutdown drain) forever; on the deadline the submit is answered
	// ESME_RSUBMITFAIL. Values <= 0 use defaultInboundSubmitTimeout.
	InboundSubmitTimeout time.Duration
	// ResponseTimeout bounds Send's wait for a response. Values <= 0 use defaultResponseTimeout.
	ResponseTimeout time.Duration
	// IdleTimeout drops a session whose peer has sent nothing for this long, reclaiming the
	// goroutine and its window slot instead of leaking them on a dead-but-open connection. It is a
	// read deadline reset before each PDU read; an expiry closes the session as an orderly shutdown
	// (Serve returns nil). Zero disables it — the zero-value Config keeps a session open
	// indefinitely. step-024 sets it (this is also the dead-peer detection standing in for the
	// enquire_link keep-alive, which is deferred). Requires a conn that supports SetReadDeadline.
	IdleTimeout time.Duration
	// Logger receives the session's structured logs. nil uses slog.Default. The message body is
	// never logged (invariant a); only its length is.
	Logger *slog.Logger

	// OnBind decides each bind. nil accepts every bind (StatusOK, SystemID = Config.SystemID).
	OnBind BindHandler
	// OnSubmit decides each submit_sm. nil answers StatusOK with an empty message id.
	OnSubmit SubmitHandler
	// OnQuery decides each query_sm. nil answers StatusOK with a zero-value state (skeleton).
	OnQuery QueryHandler
	// OnCancel decides each cancel_sm. nil answers StatusOK.
	OnCancel CancelHandler
	// OnUnbind is notified on unbind. nil is a no-op.
	OnUnbind UnbindHandler
}

// Session is one server-side SMPP session over a single connection. Create it with New and run it
// with Serve.
type Session struct {
	conn   io.ReadWriteCloser
	cfg    Config
	logger *slog.Logger

	// writeMu serialises every socket write: a net.Conn is not safe for concurrent writes, and the
	// read loop's responses race with a caller's Send without it.
	writeMu sync.Mutex

	// window is the outbound send semaphore: cap == WindowSize. serverSeq allocates the
	// sequence_number of each server-initiated request.
	window    chan struct{}
	serverSeq atomic.Uint32

	// inbound is the inbound submit_sm semaphore: cap == InboundWindow. A slot is held for the lifetime
	// of a submit worker (step-088).
	inbound chan struct{}

	// mu guards pending, the table of Send waiters keyed by sequence_number.
	mu      sync.Mutex
	pending map[uint32]chan smpp.PDU

	// st is the state machine, owned exclusively by the Serve goroutine.
	st state

	// workerCtx bounds the in-flight submit workers (step-088); cancelWork cancels it on shutdown so a
	// force-disconnect or unbind drains a slow-but-cancellable produce promptly, not only on its timeout.
	workerCtx  context.Context
	cancelWork context.CancelFunc

	closeOnce sync.Once
	done      chan struct{}
	wg        sync.WaitGroup
}

// New creates a session over an already-established connection. It performs no I/O; call Serve to
// start the read loop.
func New(conn io.ReadWriteCloser, cfg Config) *Session {
	if cfg.WindowSize < 1 {
		cfg.WindowSize = 1
	}
	if cfg.InboundWindow < 1 {
		cfg.InboundWindow = defaultInboundWindow
	}
	if cfg.InboundSubmitTimeout <= 0 {
		cfg.InboundSubmitTimeout = defaultInboundSubmitTimeout
	}
	if cfg.ResponseTimeout <= 0 {
		cfg.ResponseTimeout = defaultResponseTimeout
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	workerCtx, cancelWork := context.WithCancel(context.Background())
	return &Session{
		conn:       conn,
		cfg:        cfg,
		logger:     logger,
		window:     make(chan struct{}, cfg.WindowSize),
		inbound:    make(chan struct{}, cfg.InboundWindow),
		pending:    make(map[uint32]chan smpp.PDU),
		workerCtx:  workerCtx,
		cancelWork: cancelWork,
		done:       make(chan struct{}),
		st:         stOpen,
	}
}

// Serve runs the read loop until the ESME unbinds, the peer closes the connection, ctx is cancelled
// or a socket error occurs. It returns nil on an orderly close (unbind, EOF or ctx cancellation)
// and a non-nil error only on a genuine socket fault. Serve blocks; it is meant to run on the
// connection's own goroutine.
func (s *Session) Serve(ctx context.Context) error {
	s.wg.Add(1)
	go s.watch(ctx)

	err := s.readLoop(ctx)

	s.shutdown()
	s.wg.Wait()
	return err
}

// watch cancels the blocking ReadPDU when ctx ends by closing the connection: ReadPDU is not
// context-aware, so closing the socket is how a cancellation unblocks it. It exits on shutdown.
func (s *Session) watch(ctx context.Context) {
	defer s.wg.Done()
	select {
	case <-ctx.Done():
		// Close, not shutdown: a cancelled context is a pod drain, and a draining pod owes its ESME an
		// unbind rather than a bare FIN (guide de codage §5 [MUST], spec §6.3). Without it a rolling
		// deploy is indistinguishable from a network fault, and the ESME reconnects on its error
		// backoff instead of immediately. sendUnbind is best-effort and bounded by unbindWriteTimeout,
		// so an unresponsive peer delays this drain by at most that, never indefinitely.
		//
		// cancelWork runs FIRST, ahead of the unbind: releasing the in-flight submit workers is the
		// urgent half of a drain, and making it queue behind a courtesy write to a peer that may not be
		// reading would hold them for the whole write deadline. It is idempotent, so shutdown's own
		// call below is harmless.
		s.cancelWork()
		s.Close()
	case <-s.done:
	}
}

// write serialises a PDU onto the socket. It reports ErrClosed once the session is shutting down so
// callers do not treat a deliberate close as a fault.
func (s *Session) write(pdu smpp.PDU) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.isClosing() {
		return ErrClosed
	}
	return smpp.WritePDU(s.conn, pdu)
}

// reply writes a response PDU, logging a genuine write failure. A failure during shutdown is
// expected and stays silent. The command id is safe to log; the body never is.
func (s *Session) reply(pdu smpp.PDU) {
	if err := s.write(pdu); err != nil && !s.isClosing() {
		s.logger.Error("session: write failed", "err", err, "command", pdu.CommandID())
	}
}

// Close terminates a session from the server side: it makes a best-effort attempt to send an
// outbound unbind — so a well-behaved ESME sees an orderly close rather than a bare socket reset —
// then closes the connection, which makes Serve return. It is how step-032 force-disconnects a bind
// whose authorization has ceased (grace lapse, revocation, suspension). It never waits for the
// unbind_resp (a silent peer must not hold the close open) and never blocks indefinitely on the
// write (bounded by unbindWriteTimeout). Close is idempotent and safe to call from another
// goroutine: a second call, or a call racing the session's own teardown, is a no-op.
func (s *Session) Close() {
	s.sendUnbind()
	s.shutdown()
}

// sendUnbind writes a single server-initiated unbind, bounded by a write deadline when the
// connection supports one. A write failure (closed session, timed-out peer) is deliberately ignored:
// the unbind is a courtesy, and shutdown closes the socket regardless.
//
// The deadline is set OUTSIDE writeMu, on purpose. If the read loop holds writeMu blocked in a write
// to a stalled peer, sendUnbind cannot acquire the lock; setting the connection-wide deadline first
// unblocks that wedged write so Close never hangs indefinitely (the non-blocking guarantee). The cost
// is that a concurrent in-progress write on a session being destroyed may be cut at the deadline —
// acceptable, since the socket is about to close anyway. Do NOT move this inside write()/writeMu: that
// reintroduces the unbounded block this is here to prevent.
func (s *Session) sendUnbind() {
	if d, ok := s.conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
		_ = d.SetWriteDeadline(time.Now().Add(unbindWriteTimeout))
		defer func() { _ = d.SetWriteDeadline(time.Time{}) }()
	}
	_ = s.write(smpp.PDU{Sequence: s.serverSeq.Add(1), Body: &smpp.Unbind{}})
}

func (s *Session) shutdown() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.cancelWork() // cancel in-flight submit workers so the drain does not wait on their timeout
		_ = s.conn.Close()
	})
}

func (s *Session) isClosing() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}
