package smppserver

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"

	"github.com/martialanouman/go-gateway/internal/bindthrottle"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// fakeThrottle is a scripted BindThrottle recording how it was called.
type fakeThrottle struct {
	dec       bindthrottle.Decision
	checkErr  error
	checks    int
	records   int
	resets    int
	lastReset string
}

func (f *fakeThrottle) Check(context.Context, string, string) (bindthrottle.Decision, error) {
	f.checks++
	return f.dec, f.checkErr
}

func (f *fakeThrottle) RecordFailure(context.Context, string, string) error {
	f.records++
	return nil
}

func (f *fakeThrottle) Reset(_ context.Context, systemID string) error {
	f.resets++
	f.lastReset = systemID
	return nil
}

// trackedStore is a CredentialStore that records whether authentication was reached at all — the
// point of the throttle is to short-circuit before it.
type trackedStore struct {
	cred  cp.BindCredential
	found bool
	err   error
	calls int
}

func (s *trackedStore) BindCredentialBySystemID(context.Context, string) (cp.BindCredential, bool, error) {
	s.calls++
	return s.cred, s.found, s.err
}

// fakeRegistry accepts or refuses a token per its script.
type fakeRegistry struct {
	accepted bool
	unbinds  int
}

func (f *fakeRegistry) Bind(context.Context, *registrypb.BindRequest, ...grpc.CallOption) (*registrypb.BindResponse, error) {
	return &registrypb.BindResponse{Accepted: f.accepted, ActiveSessions: 1}, nil
}

func (f *fakeRegistry) Unbind(context.Context, *registrypb.UnbindRequest, ...grpc.CallOption) (*registrypb.UnbindResponse, error) {
	f.unbinds++
	return &registrypb.UnbindResponse{}, nil
}

// TestOnBindThrottleBlocks: a blocked subject is refused with ESME_RINVPASWD before authentication
// runs, and the block does NOT re-count (no RecordFailure) — that would let a system_id-varying
// attacker mint unbounded Redis keys — nor reset the counter.
func TestOnBindThrottleBlocks(t *testing.T) {
	store := &trackedStore{} // would report "not found" if reached; assert it is never consulted
	ft := &fakeThrottle{dec: bindthrottle.Decision{Blocked: true, RetryAfter: 0, Failures: 7}}
	l := New(store, nil, nil, Options{Throttle: ft}, discardLog())

	res := l.onBind(context.Background(), &connState{bindID: "b1"}, "10.0.0.1", nil)(
		context.Background(), session.BindRequest{SystemID: "sid-1", Password: "pw"})

	if res.Status != errs.StatusInvalidPasswd {
		t.Fatalf("status = %#x, want ESME_RINVPASWD %#x", res.Status, errs.StatusInvalidPasswd)
	}
	if store.calls != 0 {
		t.Fatalf("credential store consulted %d times, want 0 (blocked before argon2)", store.calls)
	}
	if ft.records != 0 {
		t.Fatalf("RecordFailure calls = %d, want 0 (a blocked attempt must not re-count)", ft.records)
	}
	if ft.resets != 0 {
		t.Fatalf("Reset calls = %d, want 0", ft.resets)
	}
}

// TestOnBindRecordsAuthFailure: an allowed-but-failing bind feeds the throttle and does not reset it.
func TestOnBindRecordsAuthFailure(t *testing.T) {
	store := &fakeStore{found: false} // unknown system_id → ESME_RINVPASWD
	ft := &fakeThrottle{dec: bindthrottle.Decision{Blocked: false}}
	l := New(store, nil, nil, Options{Throttle: ft}, discardLog())

	res := l.onBind(context.Background(), &connState{bindID: "b1"}, "10.0.0.1", nil)(
		context.Background(), session.BindRequest{SystemID: "sid-1", Password: "pw"})

	if res.Status != errs.StatusInvalidPasswd {
		t.Fatalf("status = %#x, want ESME_RINVPASWD", res.Status)
	}
	if ft.records != 1 {
		t.Fatalf("RecordFailure calls = %d, want 1", ft.records)
	}
	if ft.resets != 0 {
		t.Fatalf("Reset calls = %d, want 0", ft.resets)
	}
}

// TestOnBindSuccessResets: a successful bind clears the system_id counter (and only that system_id).
func TestOnBindSuccessResets(t *testing.T) {
	store := &fakeStore{cred: activeCred(t), found: true}
	ft := &fakeThrottle{dec: bindthrottle.Decision{Blocked: false}}
	l := New(store, &fakeRegistry{accepted: true}, nil, Options{Throttle: ft}, discardLog())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // stop the refresh goroutine started on success

	res := l.onBind(ctx, &connState{bindID: "b1"}, "10.0.0.1", nil)(
		ctx, session.BindRequest{SystemID: "sid-42", Password: testPassword, Mode: session.BindTransceiver})

	if res.Status != smpp.StatusOK {
		t.Fatalf("status = %#x, want StatusOK", res.Status)
	}
	if ft.resets != 1 || ft.lastReset != "sid-42" {
		t.Fatalf("Reset calls = %d lastReset = %q, want 1 and sid-42", ft.resets, ft.lastReset)
	}
	if ft.records != 0 {
		t.Fatalf("RecordFailure calls = %d, want 0 on success", ft.records)
	}
}

// TestOnBindFailsOpenOnThrottleError: a Redis error in Check does not block the bind — authentication
// runs and a valid bind still succeeds. A throttle outage must not take down the SMPP ingress.
func TestOnBindFailsOpenOnThrottleError(t *testing.T) {
	store := &fakeStore{cred: activeCred(t), found: true}
	ft := &fakeThrottle{checkErr: errors.New("redis down")}
	l := New(store, &fakeRegistry{accepted: true}, nil, Options{Throttle: ft}, discardLog())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	res := l.onBind(ctx, &connState{bindID: "b1"}, "10.0.0.1", nil)(
		ctx, session.BindRequest{SystemID: "sid-1", Password: testPassword, Mode: session.BindTransceiver})

	if res.Status != smpp.StatusOK {
		t.Fatalf("status = %#x, want StatusOK (fail-open)", res.Status)
	}
}

// TestThrottleSecurityEventNoSecret: the throttle security event names the system_id and IP (the task
// authorises these identifiers) but never the password.
func TestThrottleSecurityEventNoSecret(t *testing.T) {
	const sid = "UNIQUE_SID_QQQ"
	const pw = "UNIQUE_PW_ZZZ"

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ft := &fakeThrottle{dec: bindthrottle.Decision{Blocked: true, Failures: 9}}
	l := New(&fakeStore{}, nil, nil, Options{Throttle: ft}, logger)

	l.onBind(context.Background(), &connState{bindID: "b1"}, "203.0.113.7", nil)(
		context.Background(), session.BindRequest{SystemID: sid, Password: pw})

	out := buf.String()
	if !strings.Contains(out, "smpp bind throttled") {
		t.Fatalf("security event not emitted; log:\n%s", out)
	}
	if !strings.Contains(out, sid) || !strings.Contains(out, "203.0.113.7") {
		t.Fatalf("security event missing system_id/ip; log:\n%s", out)
	}
	if strings.Contains(out, pw) {
		t.Fatalf("security event leaked the password; log:\n%s", out)
	}
}

// TestThrottleBlockedMetricLabelled: a block increments the counter under the tripping subject's label
// (bounded cardinality — the label is "ip"/"system_id", never the value).
func TestThrottleBlockedMetricLabelled(t *testing.T) {
	vec := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_smpp_bind_throttle_blocked_total", Help: "h"}, []string{"subject"})
	ft := &fakeThrottle{dec: bindthrottle.Decision{Blocked: true, Subject: bindthrottle.SubjectIP, Failures: 6}}
	l := New(&fakeStore{}, nil, nil, Options{Throttle: ft, ThrottleBlocked: vec}, discardLog())

	l.onBind(context.Background(), &connState{bindID: "b1"}, "10.0.0.1", nil)(
		context.Background(), session.BindRequest{SystemID: "sid", Password: "pw"})

	if got := counterValue(t, vec.WithLabelValues(bindthrottle.SubjectIP)); got != 1 {
		t.Fatalf("blocked{subject=ip} = %v, want 1", got)
	}
	if got := counterValue(t, vec.WithLabelValues(bindthrottle.SubjectSystemID)); got != 0 {
		t.Fatalf("blocked{subject=system_id} = %v, want 0", got)
	}
}

// counterValue reads a counter's current value without pulling in prometheus/testutil (and its extra
// transitive dependency); client_model is already in the module graph.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("write metric: %v", err)
	}
	return m.GetCounter().GetValue()
}
