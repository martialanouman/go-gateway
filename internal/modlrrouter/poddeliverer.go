package modlrrouter

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
)

// AddrResolver maps a smpp-server pod_id to a dialable gRPC address. The template form is the M4
// mechanism; M12 swaps the resolver for real pod discovery behind this same interface.
type AddrResolver interface {
	Resolve(podID string) (string, error)
}

// templateResolver formats pod_id into an address via a single-"%s" template (e.g.
// "%s.smpp-server-headless:7000"). An empty template disables bind delivery.
type templateResolver struct {
	template string
}

// NewTemplateResolver builds a pod-address resolver from a "%s" template.
func NewTemplateResolver(template string) AddrResolver {
	return templateResolver{template: template}
}

func (r templateResolver) Resolve(podID string) (string, error) {
	if r.template == "" {
		return "", fmt.Errorf("modlrrouter: bind delivery disabled (empty pod address template)")
	}
	if podID == "" {
		return "", fmt.Errorf("modlrrouter: empty pod_id")
	}
	return fmt.Sprintf(r.template, podID), nil
}

// PodClients is the PodDeliverer: it dials the owning pod (a connection cached per pod_id) and calls
// its SessionRegistry.Deliver. gRPC NewClient is lazy, so an unreachable pod surfaces only when the RPC
// runs, as a status the round-robin classifies. Safe for concurrent use.
type PodClients struct {
	resolver AddrResolver
	dial     func(addr string) (*grpc.ClientConn, error)

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPodClients builds the pod delivery client over a pod-address resolver.
func NewPodClients(resolver AddrResolver) *PodClients {
	return &PodClients{
		resolver: resolver,
		dial: func(addr string) (*grpc.ClientConn, error) {
			// Pod-to-pod internal call; transport security terminates at the mesh (insecure). NewClient is
			// lazy — it opens no socket until the first Deliver.
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	}
}

// Deliver pushes pdu to bindID on podID, returning the gRPC status of SessionRegistry.Deliver. A pod
// whose address cannot be resolved (bind delivery disabled, or empty id) is reported Unavailable so the
// caller simply skips that bind.
func (p *PodClients) Deliver(ctx context.Context, podID, bindID string, pdu []byte) error {
	conn, err := p.conn(podID)
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

func (p *PodClients) conn(podID string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[podID]; ok {
		return c, nil
	}
	addr, err := p.resolver.Resolve(podID)
	if err != nil {
		return nil, err
	}
	c, err := p.dial(addr)
	if err != nil {
		return nil, fmt.Errorf("dial pod %s at %s: %w", podID, addr, err)
	}
	if p.conns == nil {
		p.conns = make(map[string]*grpc.ClientConn)
	}
	p.conns[podID] = c
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

// Lookup returns the account's live binds (pod_id, bind_id). The bind role is not stored by the
// registry; a transmitter is skipped later when Deliver refuses it.
func (r *RegistryLookup) Lookup(ctx context.Context, accountID uuid.UUID) ([]LiveBind, error) {
	resp, err := r.client.Lookup(ctx, &registrypb.LookupRequest{AccountId: accountID.String()})
	if err != nil {
		return nil, err
	}
	out := make([]LiveBind, 0, len(resp.GetSessions()))
	for _, s := range resp.GetSessions() {
		out = append(out, LiveBind{PodID: s.GetPodId(), BindID: s.GetBindId()})
	}
	return out, nil
}
