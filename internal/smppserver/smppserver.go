// Package smppserver is the SMPP bind front door: it accepts ESME connections on the SMPP port,
// authenticates each bind against the control plane, enforces the account's allowed_bind_types, and
// reserves a session token against the cross-pod SessionRegistry so max_sessions is honoured
// (invariant d). Each accepted connection is driven by internal/smpp/session, whose OnBind/OnUnbind
// hooks this package wires; submit_sm is rejected with ESME_RSUBMITFAIL until the MT pipeline lands
// (step-025).
//
// The bind never logs the system_id together with the password (§1.9); the argon2id verification is
// constant time (internal/credential). A live bind's registry token is kept fresh by a per-session
// refresh loop (a re-Bind of the same member refreshes the TTL without double-counting), and released
// on unbind, EOF, idle-drop or pod drain.
package smppserver

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/session"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
)

// CredentialStore resolves a presented SMPP system_id to its bind credential. It is satisfied by
// postgres.BindRepo. found is false for an unknown or revoked system_id, which the bind path maps to
// ESME_RINVPASWD (so a bind cannot enumerate valid system_ids).
type CredentialStore interface {
	BindCredentialBySystemID(ctx context.Context, systemID string) (cp.BindCredential, bool, error)
}

// Registry reserves and releases session tokens against the SessionRegistry, the atomic arbiter of
// max_sessions (invariant d). It is the subset of the generated pb.SessionRegistryClient the bind path
// uses; the real gRPC client satisfies it directly, and a fake satisfies it in tests. A Bind refused at
// the ceiling returns a gRPC ResourceExhausted status carrying the max_sessions_exceeded reason.
type Registry interface {
	Bind(ctx context.Context, in *registrypb.BindRequest, opts ...grpc.CallOption) (*registrypb.BindResponse, error)
	Unbind(ctx context.Context, in *registrypb.UnbindRequest, opts ...grpc.CallOption) (*registrypb.UnbindResponse, error)
}

// BindThrottle is the anti-brute-force layer the bind path consults before authentication. Check
// reports whether a bind must be refused with a backoff; RecordFailure counts an unsuccessful bind;
// Reset clears a system_id's counter after a success. *bindthrottle.Throttle satisfies it, a fake
// satisfies it in tests, and a nil BindThrottle disables throttling. Every call is treated fail-open by
// the caller: a Redis error lets the bind proceed, so an outage never takes down the SMPP ingress.
type BindThrottle interface {
	Check(ctx context.Context, systemID, ip string) (bindthrottle.Decision, error)
	RecordFailure(ctx context.Context, systemID, ip string) error
	Reset(ctx context.Context, systemID string) error
}

// Ingestor runs the shared MT ingestion sequence for a submit_sm: encode the envelope, produce it
// durably to mt.inbound, and project the accepted CDR row. *ingest.Ingestor satisfies it. It is the
// same helper the REST submit path uses, which is what makes the two surfaces reach the pipeline
// identically (protocol parity).
type Ingestor interface {
	Accept(ctx context.Context, env pipeline.InboundMT) error
}

// Canceller cancels a not-yet-dispatched message for a cancel_sm, scoped to the bind's account.
// *cancel.Canceller satisfies it. It returns a platform error whose SMPP surface the caller maps once
// via errs.SMPPStatusForError (an unknown message → ESME_RINVMSGID, an already-dispatched one →
// ESME_RCANCELFAIL); a nil error is a successful cancel (ESME_ROK).
type Canceller interface {
	Cancel(ctx context.Context, customerID, accountID, messageID uuid.UUID) error
}

// registryCallTimeout bounds a single registry RPC on the session-token lifecycle path: the periodic
// refresh Bind and the final Unbind that frees the token after a connection ends. The Unbind runs on a
// fresh context so a token is released even while the pod is draining (the connection's own context is
// already cancelled by then).
const registryCallTimeout = 5 * time.Second

// defaultRefreshInterval refreshes a live bind's registry token at half the registry's default session
// TTL, so the token is renewed twice per lifetime and never lapses under a session that is still alive.
const defaultRefreshInterval = session.DefaultSessionTTL / 2

// defaultMaxConns is the fallback concurrent-connection ceiling when Options.MaxConns is unset. It is
// generous enough not to constrain legitimate ESME fan-in, while still bounding the goroutines and file
// descriptors a flood of tarpitted binds can hold. Operators size it to their file-descriptor ulimit.
const defaultMaxConns = 16384

// Options configures a Listener. Addr, PodID and SystemID are required in production wiring; the
// timeouts fall back to sane defaults.
type Options struct {
	// Addr is the SMPP listen address, e.g. ":2775".
	Addr string
	// PodID identifies this pod in the session registry, so a token can be traced to the pod that owns
	// the connection and released when that pod drains.
	PodID string
	// SystemID is the server system_id echoed in a bind_resp.
	SystemID string
	// IdleTimeout drops a bind whose peer has gone silent, releasing its registry token. Zero leaves
	// the connection open indefinitely (the zero-value session default).
	IdleTimeout time.Duration
	// RefreshInterval is how often a live bind's registry token is refreshed by a re-Bind. Zero uses
	// defaultRefreshInterval (half the registry's default session TTL), so a token never lapses under a
	// live session.
	RefreshInterval time.Duration
	// Tracer opens the submit_sm ingestion span. Nil uses a no-op tracer, so tests need not wire one.
	Tracer trace.Tracer
	// Now supplies the submit_sm accept timestamp and the instant a rotation grace window is judged
	// against at bind; nil defaults to time.Now. Injectable for tests.
	Now func() time.Time
	// Throttle is the anti-brute-force layer consulted before each bind's authentication. Nil disables
	// throttling (bind-only tests leave it nil); production wiring passes a *bindthrottle.Throttle.
	Throttle BindThrottle
	// ThrottleBlocked counts binds refused by the throttle, labelled by the subject that tripped
	// ("system_id" or "ip" — both bounded, never the value). Nil skips the metric, so tests need not
	// wire a registry.
	ThrottleBlocked *prometheus.CounterVec
	// MaxConns caps concurrent accepted connections, bounding the goroutines and file descriptors an
	// unauthenticated peer can pin — in particular under the throttle's tarpit backoff. Zero uses
	// defaultMaxConns. It is a hard ceiling: beyond it, new connections wait in the kernel backlog.
	MaxConns int
	// Canceller cancels a not-yet-dispatched message for an enabled cancel_sm. Nil rejects every
	// enabled cancel_sm with ESME_RCANCELFAIL (bind-only tests leave it nil); production wiring passes
	// a *cancel.Canceller.
	Canceller Canceller
}

// Listener accepts SMPP connections and drives each through an authenticated bind. Construct it with
// New and run it with Run (a supervisor component).
type Listener struct {
	creds    CredentialStore
	registry Registry
	ingestor Ingestor
	opts     Options
	logger   *slog.Logger

	wg sync.WaitGroup

	// ready is closed once Run has bound the listener, publishing addr. It lets Addr report the
	// resolved port after an ephemeral (":0") bind.
	ready chan struct{}
	mu    sync.Mutex
	addr  string
}

// New returns a Listener. A nil logger uses slog.Default. RefreshInterval defaults to half the
// registry's default session TTL so a live bind's token is refreshed well before it lapses. A nil
// ingestor rejects every submit_sm with ESME_RSUBMITFAIL, which is how the bind-only tests build it;
// production wiring passes an *ingest.Ingestor so submit_sm reaches the MT pipeline.
func New(creds CredentialStore, registry Registry, ingestor Ingestor, opts Options, logger *slog.Logger) *Listener {
	if logger == nil {
		logger = slog.Default()
	}
	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = defaultRefreshInterval
	}
	if opts.Tracer == nil {
		opts.Tracer = noop.NewTracerProvider().Tracer("smppserver")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxConns <= 0 {
		opts.MaxConns = defaultMaxConns
	}
	return &Listener{creds: creds, registry: registry, ingestor: ingestor, opts: opts, logger: logger, ready: make(chan struct{})}
}

// Addr blocks until Run has bound the listener, then returns its resolved address, or ctx's error if
// it is cancelled first. It is meant for callers that bound an ephemeral port (":0"), chiefly tests.
func (l *Listener) Addr(ctx context.Context) (string, error) {
	select {
	case <-l.ready:
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.addr, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
