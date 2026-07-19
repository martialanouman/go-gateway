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

	// mu guards pending, the table of Send waiters keyed by sequence_number.
	mu      sync.Mutex
	pending map[uint32]chan smpp.PDU

	// st is the state machine, owned exclusively by the Serve goroutine.
	st state

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
	if cfg.ResponseTimeout <= 0 {
		cfg.ResponseTimeout = defaultResponseTimeout
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Session{
		conn:    conn,
		cfg:     cfg,
		logger:  logger,
		window:  make(chan struct{}, cfg.WindowSize),
		pending: make(map[uint32]chan smpp.PDU),
		done:    make(chan struct{}),
		st:      stOpen,
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
		s.shutdown()
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

func (s *Session) shutdown() {
	s.closeOnce.Do(func() {
		close(s.done)
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
