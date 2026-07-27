package breaker

// SMPP command_status values relevant to breaker classification (SMPP v3.4 §5.1.3).
const (
	statusOK         uint32 = 0x00000000 // ESME_ROK
	statusInvMsgLen  uint32 = 0x00000001 // ESME_RINVMSGLEN — malformed request (message-level)
	statusInvSrcAddr uint32 = 0x0000000A // ESME_RINVSRCADR (message-level)
	statusInvDstAddr uint32 = 0x0000000B // ESME_RINVDSTADR (message-level)
	statusMsgQFull   uint32 = 0x00000014 // ESME_RMSGQFUL — SMSC queue full (transient load)
	statusThrottled  uint32 = 0x00000058 // ESME_RTHROTTLED — rate limited (AIMD's concern)
)

// Outcome is how a send result feeds the breaker.
type Outcome int

// The classification outcomes.
const (
	// Success is a clean submit_sm_resp (ESME_ROK): counts toward closing / window totals.
	Success Outcome = iota
	// Failure is a connector-HEALTH fault (SMSC system error, bind failure, a transport timeout/drop):
	// it feeds the failure rate that trips the breaker.
	Failure
	// Transient is a load/throttle response (ESME_RTHROTTLED, queue full) or a message-level reject
	// (invalid dest/src/len) — the connector is healthy, so it must NOT count as a breaker failure.
	Transient
)

// Classify maps an SMPP command_status to a breaker Outcome. It draws the line the breaker cares about:
// connector HEALTH (Failure) versus load/throttle and per-message rejects (Transient). A transport-level
// error (no status — a timeout or dropped connection) is a connector-health Failure and is fed directly
// via RecordFailure by the caller, not through here.
func Classify(commandStatus uint32) Outcome {
	switch commandStatus {
	case statusOK:
		return Success
	case statusThrottled, statusMsgQFull, statusInvSrcAddr, statusInvDstAddr, statusInvMsgLen:
		return Transient
	default:
		// ESME_RSYSERR, ESME_RBINDFAIL, ESME_RINVBNDSTS, ESME_RSUBMITFAIL and other unexpected statuses
		// reflect an unhealthy connector.
		return Failure
	}
}

// Record feeds a submit_sm_resp command_status into the breaker via Classify: a Success or a health
// Failure moves the state machine; a Transient result does not signal health, so it is not counted —
// but if it came back for a half-open probe it frees that probe's slot so another trial can be tried
// (the HalfOpenTimeout still guards against probes that never resolve).
func (b *Breaker) Record(commandStatus uint32) {
	switch Classify(commandStatus) {
	case Success:
		b.RecordSuccess()
	case Failure:
		b.RecordFailure()
	case Transient:
		b.recordTransientProbe()
	}
}

// recordTransientProbe frees a half-open probe slot for an inconclusive (Transient) probe result, so the
// breaker can try another probe rather than stall on an exhausted quota.
func (b *Breaker) recordTransientProbe() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refresh()
	if b.state == HalfOpen && b.probesOut > 0 {
		b.probesOut--
	}
}
