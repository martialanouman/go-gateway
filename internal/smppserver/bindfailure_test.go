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

	if res.Status != errs.StatusBindFail || len(log.records) != 1 {
		t.Fatalf("status = %#x records = %d, want ESME_RBINDFAIL and one attempt", res.Status, len(log.records))
	}
}
