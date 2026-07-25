package smppserver

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// fakeStore is a CredentialStore returning a scripted result.
type fakeStore struct {
	cred  cp.BindCredential
	found bool
	err   error
}

func (f fakeStore) BindCredentialBySystemID(context.Context, string) (cp.BindCredential, bool, error) {
	return f.cred, f.found, f.err
}

const testPassword = "s3cr3t-bind-pw"

// activeCred is a fully valid transceiver credential; each test mutates one field to isolate a
// rejection reason.
func activeCred(t *testing.T) cp.BindCredential {
	t.Helper()
	hash, err := credential.HashBindPassword(testPassword)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return cp.BindCredential{
		PasswordHash:     hash,
		CredentialStatus: cp.CredentialActive,
		SMPPEnabled:      true,
		AllowedBindType:  cp.BindTRX,
		MaxSessions:      1,
		AccountStatus:    cp.AccountActive,
		CustomerStatus:   cp.CustomerActive,
	}
}

func TestAuthorize(t *testing.T) {
	base := activeCred(t)

	tests := []struct {
		name  string
		store fakeStore
		mode  session.BindMode
		want  uint32
	}{
		{
			name:  "valid transceiver bind",
			store: fakeStore{cred: base, found: true},
			mode:  session.BindTransceiver,
			want:  smpp.StatusOK,
		},
		{
			name:  "unknown system_id is invalid password",
			store: fakeStore{found: false},
			mode:  session.BindTransceiver,
			want:  errs.StatusInvalidPasswd,
		},
		{
			name:  "lookup error is system error",
			store: fakeStore{err: errors.New("db down")},
			mode:  session.BindTransceiver,
			want:  errs.StatusSysErr,
		},
		{
			name:  "wrong password is invalid password",
			store: fakeStore{cred: base, found: true},
			mode:  session.BindTransceiver,
			want:  errs.StatusInvalidPasswd,
		},
		{
			name:  "malformed stored hash is invalid password",
			store: fakeStore{cred: mutate(base, func(c *cp.BindCredential) { c.PasswordHash = "not-a-phc" }), found: true},
			mode:  session.BindTransceiver,
			want:  errs.StatusInvalidPasswd,
		},
		{
			name:  "disabled credential is bind fail",
			store: fakeStore{cred: mutate(base, func(c *cp.BindCredential) { c.CredentialStatus = cp.CredentialDisabled }), found: true},
			mode:  session.BindTransceiver,
			want:  errs.StatusBindFail,
		},
		{
			name:  "smpp channel disabled is bind fail",
			store: fakeStore{cred: mutate(base, func(c *cp.BindCredential) { c.SMPPEnabled = false }), found: true},
			mode:  session.BindTransceiver,
			want:  errs.StatusBindFail,
		},
		{
			name:  "suspended account is bind fail",
			store: fakeStore{cred: mutate(base, func(c *cp.BindCredential) { c.AccountStatus = cp.AccountSuspended }), found: true},
			mode:  session.BindTransceiver,
			want:  errs.StatusBindFail,
		},
		{
			name:  "suspended customer is bind fail",
			store: fakeStore{cred: mutate(base, func(c *cp.BindCredential) { c.CustomerStatus = cp.CustomerSuspended }), found: true},
			mode:  session.BindTransceiver,
			want:  errs.StatusBindFail,
		},
		{
			name:  "bind type mismatch is bind fail",
			store: fakeStore{cred: base, found: true}, // account is trx
			mode:  session.BindTransmitter,
			want:  errs.StatusBindFail,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := New(tc.store, nil, nil, Options{}, slog.New(slog.DiscardHandler))
			pw := testPassword
			if tc.name == "wrong password is invalid password" {
				pw = "wrong"
			}
			_, got, _ := l.authorize(context.Background(), session.BindRequest{
				Mode:     tc.mode,
				SystemID: "sid-1",
				Password: pw,
			})
			if got != tc.want {
				t.Errorf("authorize() status = %#x, want %#x", got, tc.want)
			}
		})
	}
}

const rotatedPassword = "r0t4t3d-bind-pw"

// graceNow is the instant every rotation-grace test is anchored on, so a credential's window can be
// placed before or after "now" without touching the wall clock.
var graceNow = time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

// rotatedCred is a credential whose secret has just been rotated: PasswordHash holds the NEW secret
// and PreviousSecretHash the old one, with a grace window ending at expiry.
func rotatedCred(t *testing.T, expiry *time.Time) cp.BindCredential {
	t.Helper()
	c := activeCred(t)
	newHash, err := credential.HashBindPassword(rotatedPassword)
	if err != nil {
		t.Fatalf("hash rotated password: %v", err)
	}
	old := c.PasswordHash // activeCred hashes testPassword
	c.PasswordHash = newHash
	c.PreviousSecretHash = &old
	c.GraceExpiresAt = expiry
	return c
}

// TestAuthorizeRotationGrace covers the rotation grace window at the bind (step-027): during the
// window BOTH secrets bind; once it closes the previous secret is dead, permanently. The clock is
// injected, so the cut-off is proven without sleeping.
func TestAuthorizeRotationGrace(t *testing.T) {
	open := graceNow.Add(time.Hour)    // window still open at graceNow
	closed := graceNow.Add(-time.Hour) // window expired an hour ago

	tests := []struct {
		name     string
		cred     cp.BindCredential
		password string
		want     uint32
	}{
		{
			name:     "previous secret binds during the grace window",
			cred:     rotatedCred(t, &open),
			password: testPassword,
			want:     smpp.StatusOK,
		},
		{
			name:     "previous secret is refused once the window has closed",
			cred:     rotatedCred(t, &closed),
			password: testPassword,
			want:     errs.StatusInvalidPasswd,
		},
		{
			name:     "new secret binds while the window is open",
			cred:     rotatedCred(t, &open),
			password: rotatedPassword,
			want:     smpp.StatusOK,
		},
		{
			name:     "new secret binds after the window has closed",
			cred:     rotatedCred(t, &closed),
			password: rotatedPassword,
			want:     smpp.StatusOK,
		},
		{
			name:     "a previous hash with no expiry never authenticates",
			cred:     mutate(rotatedCred(t, &open), func(c *cp.BindCredential) { c.GraceExpiresAt = nil }),
			password: testPassword,
			want:     errs.StatusInvalidPasswd,
		},
		{
			name:     "an expiry with no previous hash falls through to a plain rejection",
			cred:     mutate(rotatedCred(t, &open), func(c *cp.BindCredential) { c.PreviousSecretHash = nil }),
			password: testPassword,
			want:     errs.StatusInvalidPasswd,
		},
		{
			name:     "an empty previous hash never authenticates",
			cred:     mutate(rotatedCred(t, &open), func(c *cp.BindCredential) { empty := ""; c.PreviousSecretHash = &empty }),
			password: "",
			want:     errs.StatusInvalidPasswd,
		},
		{
			name:     "a malformed previous hash is refused, not trusted",
			cred:     mutate(rotatedCred(t, &open), func(c *cp.BindCredential) { bad := "not-a-phc"; c.PreviousSecretHash = &bad }),
			password: testPassword,
			want:     errs.StatusInvalidPasswd,
		},
		{
			name:     "a wrong secret is refused even with the window open",
			cred:     rotatedCred(t, &open),
			password: "neither-of-them",
			want:     errs.StatusInvalidPasswd,
		},
		{
			name:     "the window is exclusive at its expiry instant",
			cred:     rotatedCred(t, &graceNow),
			password: testPassword,
			want:     errs.StatusInvalidPasswd,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{Now: func() time.Time { return graceNow }}
			l := New(fakeStore{cred: tc.cred, found: true}, nil, nil, opts, slog.New(slog.DiscardHandler))
			_, got, _ := l.authorize(context.Background(), session.BindRequest{
				Mode:     session.BindTransceiver,
				SystemID: "sid-1",
				Password: tc.password,
			})
			if got != tc.want {
				t.Errorf("authorize() status = %#x, want %#x", got, tc.want)
			}
		})
	}
}

// TestAuthorizeGraceNeverLogsSecrets drives the grace paths that log (a malformed previous hash) and
// asserts neither the system_id nor either secret reaches the log (§1.9, invariant a).
func TestAuthorizeGraceNeverLogsSecrets(t *testing.T) {
	const sid = "UNIQUE_SYSTEM_ID_XYZ"
	const pw = "UNIQUE_PASSWORD_XYZ"

	open := graceNow.Add(time.Hour)
	cred := mutate(rotatedCred(t, &open), func(c *cp.BindCredential) { bad := "not-a-phc"; c.PreviousSecretHash = &bad })

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	opts := Options{Now: func() time.Time { return graceNow }}
	l := New(fakeStore{cred: cred, found: true}, nil, nil, opts, logger)
	l.authorize(context.Background(), session.BindRequest{SystemID: sid, Password: pw})

	if strings.Contains(buf.String(), sid) {
		t.Errorf("log contains the system_id %q", sid)
	}
	if strings.Contains(buf.String(), pw) {
		t.Errorf("log contains the password %q", pw)
	}
	if strings.Contains(buf.String(), testPassword) || strings.Contains(buf.String(), rotatedPassword) {
		t.Error("log contains a bind secret")
	}
}

// TestAuthorizeNeverLogsSecrets drives the logging paths (lookup error, malformed hash) with a
// distinctive system_id and password and asserts neither reaches the log (§1.9, invariant a).
func TestAuthorizeNeverLogsSecrets(t *testing.T) {
	const sid = "UNIQUE_SYSTEM_ID_XYZ"
	const pw = "UNIQUE_PASSWORD_XYZ"

	stores := []fakeStore{
		{err: errors.New("db down")},
		{cred: mutate(activeCred(t), func(c *cp.BindCredential) { c.PasswordHash = "not-a-phc" }), found: true},
		{cred: activeCred(t), found: true}, // wrong password path (no log, but assert anyway)
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	for _, s := range stores {
		l := New(s, nil, nil, Options{}, logger)
		l.authorize(context.Background(), session.BindRequest{SystemID: sid, Password: pw})
	}

	if strings.Contains(buf.String(), sid) {
		t.Errorf("log contains the system_id %q", sid)
	}
	if strings.Contains(buf.String(), pw) {
		t.Errorf("log contains the password %q", pw)
	}
}

func TestRegistryBindStatus(t *testing.T) {
	quota := func() error {
		st := status.New(codes.ResourceExhausted, errs.ErrMaxSessionsExceeded.String())
		st, err := st.WithDetails(&errdetails.ErrorInfo{Reason: errs.ErrMaxSessionsExceeded.String()})
		if err != nil {
			t.Fatalf("attach detail: %v", err)
		}
		return st.Err()
	}

	tests := []struct {
		name string
		err  error
		want uint32
	}{
		{"quota exceeded with reason detail", quota(), errs.StatusBindFail},
		{"resource exhausted without detail", status.Error(codes.ResourceExhausted, "full"), errs.StatusBindFail},
		{"internal error is system error", status.Error(codes.Internal, "boom"), errs.StatusSysErr},
		{"non-status error is system error", errors.New("plain"), errs.StatusSysErr},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := registryBindStatus(tc.err); got != tc.want {
				t.Errorf("registryBindStatus() = %#x, want %#x", got, tc.want)
			}
		})
	}
}

// mutate returns a copy of c with fn applied, so each table row starts from a valid credential.
func mutate(c cp.BindCredential, fn func(*cp.BindCredential)) cp.BindCredential {
	fn(&c)
	return c
}
