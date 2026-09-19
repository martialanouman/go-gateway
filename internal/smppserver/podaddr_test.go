package smppserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// capturingRegistry records every BindRequest, so a test can read what the pod published.
type capturingRegistry struct {
	mu   sync.Mutex
	reqs []*registrypb.BindRequest
}

func (r *capturingRegistry) Bind(_ context.Context, in *registrypb.BindRequest, _ ...grpc.CallOption) (*registrypb.BindResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, in)
	return &registrypb.BindResponse{Accepted: true, ActiveSessions: 1}, nil
}

func (r *capturingRegistry) Unbind(context.Context, *registrypb.UnbindRequest, ...grpc.CallOption) (*registrypb.UnbindResponse, error) {
	return &registrypb.UnbindResponse{}, nil
}

func (r *capturingRegistry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reqs)
}

func (r *capturingRegistry) last() *registrypb.BindRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reqs[len(r.reqs)-1]
}

const testPodAddr = "10.9.8.7:7000"

// TestBindPublishesThePodsDialAddress is the first half of step-302 on this side: the address the pod
// announces to the registry is what the return path will dial.
func TestBindPublishesThePodsDialAddress(t *testing.T) {
	reg := &capturingRegistry{}
	l := New(&fakeStore{cred: activeCred(t), found: true}, reg, nil,
		Options{PodID: "pod-1", PodAddr: testPodAddr}, discardLog())

	res := l.onBind(context.Background(), &connState{bindID: "b1"}, "10.0.0.1", nil)(
		context.Background(), session.BindRequest{SystemID: "sid-1", Password: testPassword, Mode: session.BindTransceiver})
	if res.Status != 0 {
		t.Fatalf("bind refused with status %#x, want accepted", res.Status)
	}

	if reg.count() != 1 {
		t.Fatalf("registry saw %d binds, want 1", reg.count())
	}
	if got := reg.last().GetSession().GetPodAddr(); got != testPodAddr {
		t.Errorf("bind published pod_addr %q, want %q", got, testPodAddr)
	}
}

// TestTokenRefreshRepublishesTheDialAddress is the half that bites later and quieter. The registry
// expires a pod's address with the session TTL, and refreshLoop's re-Bind is the only thing that
// renews it: a refresh that omits the address lets it lapse ~60 s after the bind while the session
// lives on, and every MO for that ESME silently falls through to the webhook from then on.
func TestTokenRefreshRepublishesTheDialAddress(t *testing.T) {
	reg := &capturingRegistry{}
	l := New(&fakeStore{cred: activeCred(t), found: true}, reg, nil,
		Options{PodID: "pod-1", PodAddr: testPodAddr, RefreshInterval: 5 * time.Millisecond}, discardLog())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	st := &connState{accountID: uuid.New(), systemID: "sid-1", bindID: "b1", maxSessions: 2}
	go l.refreshLoop(ctx, st, session.BindTransceiver, done)

	deadline := time.After(2 * time.Second)
	for reg.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("no token refresh within 2s")
		case <-time.After(time.Millisecond):
		}
	}
	got := reg.last().GetSession().GetPodAddr()
	cancel()
	<-done

	if got != testPodAddr {
		t.Errorf("token refresh published pod_addr %q, want %q — an address that stops being "+
			"refreshed expires under a live session, and the return path goes quiet", got, testPodAddr)
	}
}
