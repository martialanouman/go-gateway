package modlrrouter

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
)

// PodClients is the PodDeliverer: it dials the address the owning pod published for itself (a
// connection cached per address) and calls its SessionRegistry.Deliver. gRPC NewClient is lazy, so an
// unreachable pod surfaces only when the RPC runs, as a status the round-robin classifies. Safe for
// concurrent use.
//
// The connection is cached per ADDRESS, not per pod_id: the address is what identifies a transport,
// and a pod_id that outlived its address would otherwise pin a connection to somewhere the pod is not.
type PodClients struct {
	dial func(addr string) (*grpc.ClientConn, error)

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPodClients builds the pod delivery client. dial carries the pod's TLS
// identity and the name it verifies, which is not the address dialled: a pod's certificate is issued per
// Deployment, so it can only attest that the peer belongs to it. Whether it is the RIGHT pod is
// Deliver's answer. It is lazy — no socket opens until the first Deliver.
func NewPodClients(dial func(addr string) (*grpc.ClientConn, error)) *PodClients {
	return &PodClients{dial: dial}
}

// Deliver pushes pdu to the bind, dialling the address the registry carries for its pod and returning
// the gRPC status of SessionRegistry.Deliver. A bind whose pod published no address is reported
// Unavailable so the caller simply skips it — the state a replica from before step-302 is in, for the
// length of one rollout.
func (p *PodClients) Deliver(ctx context.Context, bind LiveBind, pdu []byte) error {
	podID, bindID := bind.PodID, bind.BindID
	if bind.Addr == "" {
		return status.Errorf(codes.Unavailable, "modlrrouter: pod %s published no dial address", podID)
	}
	conn, err := p.conn(podID, bind.Addr)
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	resp, err := registrypb.NewSessionRegistryClient(conn).Deliver(ctx,
		&registrypb.DeliverRequest{BindId: bindID, Pdu: pdu})
	if err != nil {
		return err
	}
	if !resp.GetDelivered() {
		return status.Error(codes.Unavailable, "modlrrouter: pod reported not delivered")
	}
	return nil
}

func (p *PodClients) conn(podID, addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := p.dial(addr)
	if err != nil {
		return nil, fmt.Errorf("dial pod %s at %s: %w", podID, addr, err)
	}
	if p.conns == nil {
		p.conns = make(map[string]*grpc.ClientConn)
	}
	p.conns[addr] = c
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
