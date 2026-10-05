package smppserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/bindfailure"
	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

type recordedFailure struct {
	account uuid.UUID
	failure bindfailure.Failure
}

type fakeFailureLog struct {
	records []recordedFailure
	err     error
}

func (f *fakeFailureLog) Record(_ context.Context, accountID uuid.UUID, failure bindfailure.Failure) error {
	f.records = append(f.records, recordedFailure{accountID, failure})
	return f.err
}

type erroringRegistry struct{ err error }

func (r erroringRegistry) Bind(context.Context, *registrypb.BindRequest, ...grpc.CallOption) (*registrypb.BindResponse, error) {
	return nil, r.err
}

func (r erroringRegistry) Unbind(context.Context, *registrypb.UnbindRequest, ...grpc.CallOption) (*registrypb.UnbindResponse, error) {
	return &registrypb.UnbindResponse{}, nil
}

func maxSessionsError(t *testing.T) error {
	t.Helper()
	st, err := status.New(codes.ResourceExhausted, "full").WithDetails(&errdetails.ErrorInfo{Reason: errs.ErrMaxSessionsExceeded.String()})
	if err != nil {
		t.Fatalf("status detail: %v", err)
	}
	return st.Err()
}

var failureNow = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func TestOnBindRecordsEachRefusalAgainstItsAccount(t *testing.T) {
	account := uuid.New()
	resolved := activeCred(t)
	resolved.AccountID = account

	tests := []struct {
		name       string
		store      fakeStore
		throttle   bindthrottle.Decision
		registry   Registry
		password   string
		wantStatus uint32
		want       []recordedFailure
	}{
		{
			name:       "authorize refusal",
			store:      fakeStore{cred: mutate(resolved, func(c *cp.BindCredential) { c.SMPPEnabled = false }), found: true},
			password:   testPassword,
			wantStatus: errs.StatusBindFail,
			want: []recordedFailure{{account, bindfailure.Failure{At: failureNow, RemoteIP: "203.0.113.7", BindType: "trx",
				CommandStatus: "ESME_RBINDFAIL", Reason: bindfailure.ReasonSMPPChannelDisabled}}},
		},
		{
			name:       "wrong password",
			store:      fakeStore{cred: resolved, found: true},
			password:   "wrong",
			wantStatus: errs.StatusInvalidPasswd,
			want: []recordedFailure{{account, bindfailure.Failure{At: failureNow, RemoteIP: "203.0.113.7", BindType: "trx",
				CommandStatus: "ESME_RINVPASWD", Reason: bindfailure.ReasonPasswordMismatch}}},
		},
		{
			name:       "throttled bind, even with the right secret",
			store:      fakeStore{cred: resolved, found: true},
			throttle:   bindthrottle.Decision{Blocked: true},
			password:   testPassword,
			wantStatus: errs.StatusInvalidPasswd,
			want: []recordedFailure{{account, bindfailure.Failure{At: failureNow, RemoteIP: "203.0.113.7", BindType: "trx",
				CommandStatus: "ESME_RINVPASWD", Reason: bindfailure.ReasonThrottled}}},
		},
		{
			name:       "max_sessions reached",
			store:      fakeStore{cred: resolved, found: true},
			registry:   erroringRegistry{err: maxSessionsError(t)},
			password:   testPassword,
			wantStatus: errs.StatusBindFail,
			want: []recordedFailure{{account, bindfailure.Failure{At: failureNow, RemoteIP: "203.0.113.7", BindType: "trx",
				CommandStatus: "ESME_RBINDFAIL", Reason: bindfailure.ReasonMaxSessions}}},
		},
		{
			name:       "registry unavailable",
			store:      fakeStore{cred: resolved, found: true},
			registry:   erroringRegistry{err: status.Error(codes.Unavailable, "down")},
			password:   testPassword,
			wantStatus: errs.StatusSysErr,
			want: []recordedFailure{{account, bindfailure.Failure{At: failureNow, RemoteIP: "203.0.113.7", BindType: "trx",
				CommandStatus: "ESME_RSYSERR", Reason: bindfailure.ReasonRegistryUnavailable}}},
		},
		{
			name:       "unknown system_id belongs to no account",
			store:      fakeStore{found: false},
			password:   testPassword,
			wantStatus: errs.StatusInvalidPasswd,
		},
		{
			name:       "throttled unknown system_id belongs to no account",
			store:      fakeStore{found: false},
			throttle:   bindthrottle.Decision{Blocked: true},
			password:   testPassword,
			wantStatus: errs.StatusInvalidPasswd,
		},
		{
			name:       "lookup outage resolves nothing",
			store:      fakeStore{err: errors.New("db down")},
			password:   testPassword,
			wantStatus: errs.StatusSysErr,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			log := &fakeFailureLog{}
			opts := Options{
				Throttle:     &fakeThrottle{dec: tc.throttle},
				BindFailures: log,
				Now:          func() time.Time { return failureNow },
			}
			l := New(tc.store, tc.registry, nil, opts, discardLog())

			res := l.onBind(context.Background(), &connState{bindID: "b1"}, "203.0.113.7", nil)(
				context.Background(), session.BindRequest{SystemID: "sid-1", Password: tc.password, Mode: session.BindTransceiver})
			l.wg.Wait()

			if res.Status != tc.wantStatus {
				t.Fatalf("status = %#x, want %#x", res.Status, tc.wantStatus)
			}
			if len(log.records) != len(tc.want) {
				t.Fatalf("records = %+v, want %+v", log.records, tc.want)
			}
			for i := range tc.want {
				if log.records[i] != tc.want[i] {
					t.Fatalf("record = %+v, want %+v", log.records[i], tc.want[i])
				}
			}
		})
	}
}

func TestOnBindFailureLogOutageLeavesTheAnswerUnchanged(t *testing.T) {
	cred := mutate(activeCred(t), func(c *cp.BindCredential) { c.CredentialStatus = cp.CredentialDisabled })
	log := &fakeFailureLog{err: errors.New("redis down")}
	l := New(fakeStore{cred: cred, found: true}, nil, nil, Options{BindFailures: log}, discardLog())

	res := l.onBind(context.Background(), &connState{bindID: "b1"}, "203.0.113.7", nil)(
		context.Background(), session.BindRequest{SystemID: "sid-1", Password: testPassword, Mode: session.BindTransceiver})
	l.wg.Wait()

	if res.Status != errs.StatusBindFail || len(log.records) != 1 {
		t.Fatalf("status = %#x records = %d, want ESME_RBINDFAIL and one attempt", res.Status, len(log.records))
	}
}

type blockingFailureLog struct {
	release chan struct{}
	records int
}

func (b *blockingFailureLog) Record(context.Context, uuid.UUID, bindfailure.Failure) error {
	<-b.release
	b.records++
	return nil
}

type blockingStore struct {
	cred    cp.BindCredential
	release chan struct{}
}

func (b blockingStore) BindCredentialBySystemID(context.Context, string) (cp.BindCredential, bool, error) {
	<-b.release
	return b.cred, true, nil
}

// The design keeps the wire unchanged, timing included: a refused bind answers before its record is
// written, or a Redis round trip would tell a revoked system_id from an unknown one (§11.3).
func TestOnBindAnswersBeforeRecordingTheRefusal(t *testing.T) {
	revoked := mutate(activeCred(t), func(c *cp.BindCredential) { c.CredentialStatus = cp.CredentialRevoked })
	log := &blockingFailureLog{release: make(chan struct{})}
	l := New(fakeStore{cred: revoked, found: true}, nil, nil, Options{BindFailures: log}, discardLog())

	answered := make(chan uint32)
	go func() {
		answered <- l.onBind(context.Background(), &connState{bindID: "b1"}, "203.0.113.7", nil)(
			context.Background(), session.BindRequest{SystemID: "sid-1", Password: testPassword, Mode: session.BindTransceiver}).Status
	}()
	select {
	case st := <-answered:
		if st != errs.StatusInvalidPasswd {
			t.Fatalf("status = %#x, want ESME_RINVPASWD", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the bind waited for its failure record")
	}
	close(log.release)
	l.wg.Wait()
	if log.records != 1 {
		t.Fatalf("records = %d, want 1 once released", log.records)
	}
}

// A throttled bind answered after its backoff alone before step-286b, whatever the system_id: the
// attributing lookup must not reopen that timing difference.
func TestThrottledBindAnswersBeforeItsAttributingLookup(t *testing.T) {
	store := blockingStore{cred: activeCred(t), release: make(chan struct{})}
	log := &fakeFailureLog{}
	l := New(store, nil, nil, Options{Throttle: &fakeThrottle{dec: bindthrottle.Decision{Blocked: true}}, BindFailures: log}, discardLog())

	answered := make(chan struct{})
	go func() {
		l.onBind(context.Background(), &connState{bindID: "b1"}, "203.0.113.7", nil)(
			context.Background(), session.BindRequest{SystemID: "sid-1", Password: testPassword, Mode: session.BindTransceiver})
		close(answered)
	}()
	select {
	case <-answered:
	case <-time.After(2 * time.Second):
		t.Fatal("the throttled bind waited for its attributing lookup")
	}
	close(store.release)
	l.wg.Wait()
	if len(log.records) != 1 || log.records[0].failure.Reason != bindfailure.ReasonThrottled {
		t.Fatalf("records = %+v, want one throttled", log.records)
	}
}
