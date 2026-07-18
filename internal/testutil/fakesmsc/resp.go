package fakesmsc

import (
	"time"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// Resp is the scripted answer to a submit_sm (strategie-de-test §2 / plan §1.8). It fixes the
// command_status the fake SMSC returns and an optional injected delay, so a test can drive the
// connector through success, throttling, a system error or a slow link.
type Resp struct {
	status uint32
	delay  time.Duration
}

// OK accepts the submission: the fake SMSC assigns a message id and returns ESME_ROK.
func OK() Resp { return Resp{status: smpp.StatusOK} }

// Throttled rejects the submission with ESME_RTHROTTLED (rate_limited on the gateway side).
func Throttled() Resp { return Resp{status: errs.StatusThrottled} }

// SysErr rejects the submission with ESME_RSYSERR.
func SysErr() Resp { return Resp{status: errs.StatusSysErr} }

// Delay accepts the submission but waits d before answering, simulating a slow link. The response
// is otherwise an OK.
func Delay(d time.Duration) Resp { return Resp{status: smpp.StatusOK, delay: d} }

// ok reports whether the response is a success.
func (r Resp) ok() bool { return r.status == smpp.StatusOK }
