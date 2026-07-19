package session

// state is the SMPP session lifecycle on the server side (SMPP v3.4 §2.2). A session opens
// unbound, transitions to one of the three bound roles on a successful bind, and reaches the
// terminal unbound state on unbind. state is owned by the Serve goroutine (the sole reader of the
// socket): it is never read or written from any other goroutine, so it needs no lock.
type state uint8

const (
	// stOpen is the state before any bind: only bind_* (and enquire_link) are accepted.
	stOpen state = iota
	// stBoundTX is a transmitter bind: the ESME may submit_sm but not receive deliver_sm.
	stBoundTX
	// stBoundRX is a receiver bind: the ESME may receive deliver_sm but not submit_sm.
	stBoundRX
	// stBoundTRX is a transceiver bind: the ESME may both submit_sm and receive deliver_sm.
	stBoundTRX
	// stUnbound is terminal: an unbind has been exchanged and the connection is closing.
	stUnbound
)

func (s state) String() string {
	switch s {
	case stOpen:
		return "open"
	case stBoundTX:
		return "bound_tx"
	case stBoundRX:
		return "bound_rx"
	case stBoundTRX:
		return "bound_trx"
	case stUnbound:
		return "unbound"
	default:
		return "unknown"
	}
}

// isBound reports whether a bind has completed (any of the three roles).
func (s state) isBound() bool {
	return s == stBoundTX || s == stBoundRX || s == stBoundTRX
}

// canSubmit reports whether submit_sm is allowed: only a transmitter or transceiver bind may
// submit (SMPP v3.4 §4.4). A submit in any other state is rejected with ESME_RINVBNDSTS.
func (s state) canSubmit() bool {
	return s == stBoundTX || s == stBoundTRX
}

// BindMode is the role an ESME requests with a bind_transmitter, bind_receiver or
// bind_transceiver. The session reports it to the OnBind hook so the caller can apply role-aware
// policy without inspecting raw PDUs.
type BindMode uint8

const (
	// BindTransmitter is a bind_transmitter: transmit-only (submit_sm).
	BindTransmitter BindMode = iota
	// BindReceiver is a bind_receiver: receive-only (deliver_sm).
	BindReceiver
	// BindTransceiver is a bind_transceiver: bidirectional.
	BindTransceiver
)

func (m BindMode) String() string {
	switch m {
	case BindTransmitter:
		return "transmitter"
	case BindReceiver:
		return "receiver"
	case BindTransceiver:
		return "transceiver"
	default:
		return "unknown"
	}
}

// stateForMode maps a bind mode to the bound state it establishes.
func stateForMode(m BindMode) state {
	switch m {
	case BindTransmitter:
		return stBoundTX
	case BindReceiver:
		return stBoundRX
	case BindTransceiver:
		return stBoundTRX
	default:
		return stOpen
	}
}
