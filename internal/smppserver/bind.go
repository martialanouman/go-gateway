package smppserver

import (
	"context"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	registrypb "github.com/martialanouman/go-gateway/internal/session/pb"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// authorize authenticates and authorises a bind against the control plane, returning the resolved
// credential and smpp.StatusOK on success, or a rejection command_status. It performs no registry
// interaction — reserving the session token (invariant d) is the caller's next step.
//
// The failure codes are deliberate (§11.3): an unknown system_id and a wrong password both answer
// ESME_RINVPASWD, so a bind cannot tell which system_ids exist; a known bind on a disabled credential
// or channel, a suspended account or a mismatched bind type answers ESME_RBINDFAIL. It never logs the
// system_id or the password (§1.9). The password comparison is constant time (internal/credential).
func (l *Listener) authorize(ctx context.Context, req session.BindRequest) (cp.BindCredential, uint32) {
	cred, found, err := l.creds.BindCredentialBySystemID(ctx, req.SystemID)
	if err != nil {
		l.logger.ErrorContext(ctx, "smpp bind: credential lookup failed", "err", err)
		return cp.BindCredential{}, errs.StatusSysErr
	}
	if !found {
		return cp.BindCredential{}, errs.StatusInvalidPasswd
	}

	ok, err := credential.VerifyBindPassword(req.Password, cred.PasswordHash)
	if err != nil {
		// A stored hash that will not parse is an operator data fault, not a client error; reject the
		// bind (never accept on a broken hash) and log without the secret.
		l.logger.ErrorContext(ctx, "smpp bind: stored password hash malformed",
			"err", err, "account_id", cred.AccountID)
		return cp.BindCredential{}, errs.StatusInvalidPasswd
	}
	if !ok {
		return cp.BindCredential{}, errs.StatusInvalidPasswd
	}

	if cred.CredentialStatus != cp.CredentialActive {
		return cp.BindCredential{}, errs.StatusBindFail
	}
	if !cred.SMPPEnabled {
		return cp.BindCredential{}, errs.StatusBindFail
	}
	if cred.EffectiveStatus() != cp.AccountActive {
		return cp.BindCredential{}, errs.StatusBindFail
	}
	if cred.AllowedBindType != bindTypeForMode(req.Mode) {
		return cp.BindCredential{}, errs.StatusBindFail
	}
	return cred, smpp.StatusOK
}

// bindTypeForMode maps a requested bind mode to the control-plane bind type. allowed_bind_types is a
// single scalar value, so the match is strict equality: a 'trx' account admits only a transceiver bind
// (an empty return for an unknown mode never equals a valid allowed type, so it rejects).
func bindTypeForMode(mode session.BindMode) cp.BindType {
	switch mode {
	case session.BindTransmitter:
		return cp.BindTX
	case session.BindReceiver:
		return cp.BindRX
	case session.BindTransceiver:
		return cp.BindTRX
	default:
		return ""
	}
}

// pbBindType maps a requested bind mode to the registry's proto BindType, carried on the Session for
// diagnostics (the registry enforces max_sessions on the count alone, regardless of type).
func pbBindType(mode session.BindMode) registrypb.BindType {
	switch mode {
	case session.BindTransmitter:
		return registrypb.BindType_BIND_TYPE_TX
	case session.BindReceiver:
		return registrypb.BindType_BIND_TYPE_RX
	case session.BindTransceiver:
		return registrypb.BindType_BIND_TYPE_TRX
	default:
		return registrypb.BindType_BIND_TYPE_UNSPECIFIED
	}
}

// registryBindStatus maps a SessionRegistry.Bind error to the SMPP command_status returned to the
// ESME. The machine-readable gateway Code travels in a google.rpc.ErrorInfo.Reason, which maps through
// errs.SMPPStatus (max_sessions_exceeded -> ESME_RBINDFAIL). A ResourceExhausted status with no usable
// detail is still a quota rejection; anything else is a server fault (ESME_RSYSERR).
func registryBindStatus(err error) uint32 {
	st, ok := status.FromError(err)
	if !ok {
		return errs.StatusSysErr
	}
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if s, ok := errs.SMPPStatus(errs.Code(info.GetReason())); ok {
			return s
		}
	}
	if st.Code() == codes.ResourceExhausted {
		return errs.StatusBindFail
	}
	return errs.StatusSysErr
}

// rejectSubmit answers every submit_sm with ESME_RSUBMITFAIL until the MT pipeline lands (step-025).
// It never touches the message body, so nothing can leak (invariant a).
func rejectSubmit(_ context.Context, _ session.SubmitRequest) session.SubmitResult {
	return session.SubmitResult{Status: errs.StatusSubmitFail}
}
