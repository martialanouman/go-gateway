package modlrrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/session"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
)

// PodClients is the PodDeliverer: it dials the address the owning pod published for itself (a
// connection cached per address) and calls its SessionRegistry.Deliver. gRPC NewClient is lazy, so an
// unreachable pod surfaces only when the RPC runs, as a status the round-robin classifies. Safe for
// concurrent use.
//
// The connection is cached per ADDRESS, not per pod_id: the address is what identifies a transport,
// and a pod_id that outlived its address would otherwise pin a connection to somewhere the pod is not.
//
// An address no Deliver has used for evictAfter is closed and dropped: past the registry's session TTL
// no live bind can still carry it, and a ClientConn left open would reconnect on its backoff forever.
//
// ponytail: swept only on Deliver, so with no traffic at all the orphans live until the next one; a
// periodic sweep if that ever matters.
type PodClients struct {
	dial func(addr string) (*grpc.ClientConn, error)
	now  func() time.Time

	mu     sync.Mutex
	conns  map[string]*podConn
	closed bool
}

// NewPodClients builds the pod delivery client. dial carries the pod's TLS
// identity and the name it verifies, which is not the address dialled: a pod's certificate is issued per
// Deployment, so it can only attest that the peer belongs to it. Whether it is the RIGHT pod is
// Deliver's answer. It is lazy — no socket opens until the first Deliver.
func NewPodClients(dial func(addr string) (*grpc.ClientConn, error), opts ...PodClientsOption) *PodClients {
	p := &PodClients{dial: dial, now: time.Now}
	for _, o := range opts {
		o(p)
	}
	return p
}

type podConn struct {
	*grpc.ClientConn
	lastUsed time.Time
}

// evictAfter must exceed the session TTL, or a bind still listed by the registry could lose its
// connection between two of its own deliveries.
const evictAfter = 2 * session.DefaultSessionTTL

// PodClientsOption configures PodClients.
type PodClientsOption func(*PodClients)

// WithPodClientsClock overrides the clock, so tests can age a connection past the eviction window.
func WithPodClientsClock(now func() time.Time) PodClientsOption {
	return func(p *PodClients) { p.now = now }
}

// Deliver pushes pdu to the bind, dialling the address the registry carries for its pod and returning
// the gRPC status of SessionRegistry.Deliver. A bind whose pod published no address is reported
// Unavailable so the caller simply skips it — the state a replica from before step-302 is in, for the
// length of one rollout.
func (p *PodClients) Deliver(ctx context.Context, bind LiveBind, pdu []byte) error {
	if bind.Addr == "" {
		return status.Errorf(codes.Unavailable, "modlrrouter: pod %s published no dial address", bind.PodID)
	}
	conn, err := p.conn(bind)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	resp, err := registrypb.NewSessionRegistryClient(conn).Deliver(ctx,
		&registrypb.DeliverRequest{BindId: bind.BindID, Pdu: pdu})
	if err != nil {
		return err
	}
	if !resp.GetDelivered() {
		return status.Error(codes.Unavailable, "modlrrouter: pod reported not delivered")
	}
	return nil
}

func (p *PodClients) conn(bind LiveBind) (*grpc.ClientConn, error) {
	var stale []*grpc.ClientConn
	defer func() {
		for _, c := range stale {
			_ = c.Close()
		}
	}()
	p.mu.Lock()
	defer p.mu.Unlock()
	// A Deliver still in flight when the service shuts down would otherwise dial a fresh connection
	// after Close has already swept the map, and leak it.
	if p.closed {
		return nil, fmt.Errorf("pod clients closed")
	}
	now := p.now()
	for addr, c := range p.conns {
		if now.Sub(c.lastUsed) > evictAfter {
			stale = append(stale, c.ClientConn)
			delete(p.conns, addr)
		}
	}
	if c, ok := p.conns[bind.Addr]; ok {
		c.lastUsed = now
		// The address may have been handed to a new pod while the old occupant's connection sat in
		// backoff (up to 120 s). Sticky TRANSIENT_FAILURE keeps this RPC failing fast either way; the
		// reset only makes the next one find the new pod instead of waiting the backoff out.
		if c.GetState() == connectivity.TransientFailure {
			c.ResetConnectBackoff()
		}
		return c.ClientConn, nil
	}
	c, err := p.dial(bind.Addr)
	if err != nil {
		return nil, fmt.Errorf("dial pod %s at %s: %w", bind.PodID, bind.Addr, err)
	}
	if p.conns == nil {
		p.conns = make(map[string]*podConn)
	}
	p.conns[bind.Addr] = &podConn{ClientConn: c, lastUsed: now}
	return c, nil
}

// Close closes every cached pod connection.
func (p *PodClients) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
	p.closed = true
}

// RegistryLookup resolves an account's live binds via the SessionRegistry client.
type RegistryLookup struct {
	client registrypb.SessionRegistryClient
}

// NewRegistryLookup builds a lookup over the SessionRegistry client.
func NewRegistryLookup(client registrypb.SessionRegistryClient) *RegistryLookup {
	return &RegistryLookup{client: client}
}

// Lookup returns the account's live binds (pod_id, its published address, bind_id). The bind role is
// not stored by the registry; a transmitter is skipped later when Deliver refuses it.
func (r *RegistryLookup) Lookup(ctx context.Context, accountID uuid.UUID) ([]LiveBind, error) {
	resp, err := r.client.Lookup(ctx, &registrypb.LookupRequest{AccountId: accountID.String()})
	if err != nil {
		return nil, err
	}
	out := make([]LiveBind, 0, len(resp.GetSessions()))
	for _, s := range resp.GetSessions() {
		out = append(out, LiveBind{PodID: s.GetPodId(), Addr: s.GetPodAddr(), BindID: s.GetBindId()})
	}
	return out, nil
}
