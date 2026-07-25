package session_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/session"
	"github.com/martialanouman/go-gateway/internal/session/disconnect"
	"github.com/martialanouman/go-gateway/internal/session/pb"
)

// fakePublisher records what a Server publishes, so a Disconnect test needs no Redis. The Disconnect
// path never touches the Registry, so the Server under test is built with a nil registry.
type fakePublisher struct {
	mu       sync.Mutex
	channel  string
	payloads [][]byte
	err      error
}

func (f *fakePublisher) Publish(_ context.Context, channel string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.channel = channel
	f.payloads = append(f.payloads, payload)
	return nil
}

func (f *fakePublisher) only(t *testing.T) disconnect.Event {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.payloads) != 1 {
		t.Fatalf("published %d payloads, want 1", len(f.payloads))
	}
	if f.channel != disconnect.Channel {
		t.Fatalf("published on channel %q, want %q", f.channel, disconnect.Channel)
	}
	e, err := disconnect.Decode(f.payloads[0])
	if err != nil {
		t.Fatalf("decode published payload: %v", err)
	}
	return e
}

func TestServer_DisconnectPublishesAccountEvent(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{}
	srv := session.NewServer(nil, pub)

	resp, err := srv.Disconnect(context.Background(), &pb.DisconnectRequest{
		Scope:  pb.DisconnectScope_DISCONNECT_SCOPE_ACCOUNT,
		Id:     "acct-1",
		Reason: "credential_revoked",
	})
	if err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if !resp.GetPublished() {
		t.Error("published = false, want true")
	}
	if got, want := pub.only(t), (disconnect.Event{Scope: disconnect.ScopeAccount, ID: "acct-1", Reason: "credential_revoked"}); got != want {
		t.Errorf("event = %+v, want %+v", got, want)
	}
}

func TestServer_DisconnectPublishesCustomerEvent(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{}
	srv := session.NewServer(nil, pub)

	if _, err := srv.Disconnect(context.Background(), &pb.DisconnectRequest{
		Scope:  pb.DisconnectScope_DISCONNECT_SCOPE_CUSTOMER,
		Id:     "cust-9",
		Reason: "customer_suspended",
	}); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if got := pub.only(t); got.Scope != disconnect.ScopeCustomer || got.ID != "cust-9" {
		t.Errorf("event = %+v, want customer/cust-9", got)
	}
}

func TestServer_DisconnectRejectsUnspecifiedScope(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{}
	srv := session.NewServer(nil, pub)

	_, err := srv.Disconnect(context.Background(), &pb.DisconnectRequest{
		Scope: pb.DisconnectScope_DISCONNECT_SCOPE_UNSPECIFIED, Id: "x", Reason: "y",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
	if len(pub.payloads) != 0 {
		t.Error("published despite an invalid request")
	}
}

func TestServer_DisconnectRejectsEmptyID(t *testing.T) {
	t.Parallel()
	srv := session.NewServer(nil, &fakePublisher{})
	_, err := srv.Disconnect(context.Background(), &pb.DisconnectRequest{
		Scope: pb.DisconnectScope_DISCONNECT_SCOPE_ACCOUNT, Id: "", Reason: "y",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestServer_DisconnectPublishErrorIsInternal(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{err: errors.New("redis down")}
	srv := session.NewServer(nil, pub)

	_, err := srv.Disconnect(context.Background(), &pb.DisconnectRequest{
		Scope: pb.DisconnectScope_DISCONNECT_SCOPE_ACCOUNT, Id: "acct-1", Reason: "credential_revoked",
	})
	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want Internal", status.Code(err))
	}
}
