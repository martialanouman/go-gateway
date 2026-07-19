package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// readLoop reads and dispatches PDUs until the session ends. An orderly close (peer EOF, a
// shutdown-triggered net.ErrClosed, or an unbind) returns nil. A decodable-but-invalid frame is
// answered with generic_nack and the loop continues, since ReadPDU has consumed the whole frame so
// framing stays intact. Any other error is a genuine socket fault.
func (s *Session) readLoop(ctx context.Context) error {
	for {
		pdu, err := smpp.ReadPDU(s.conn)
		if err != nil {
			// A read error once we are shutting down (ctx cancel, unbind, write fault) is our own
			// close, not a fault: net.Conn yields net.ErrClosed, net.Pipe yields io.ErrClosedPipe.
			if s.isClosing() {
				return nil
			}
			switch {
			case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
				return nil
			case errors.Is(err, smpp.ErrUnknownCommand),
				errors.Is(err, smpp.ErrMalformedBody),
				errors.Is(err, smpp.ErrPDUTooLarge):
				// ReadPDU returns a zero PDU on a decode error, so the offending
				// sequence_number is unrecoverable: the nack carries sequence 0, which is
				// correct for a frame that could not be correlated.
				s.reply(smpp.PDU{Status: nackStatus(err), Body: &smpp.GenericNACK{}})
				continue
			default:
				return fmt.Errorf("session: read: %w", err)
			}
		}
		if done := s.handle(ctx, pdu); done {
			return nil
		}
	}
}

// handle applies one decoded PDU to the state machine and returns true when the session must close
// (after unbind). It runs on the read goroutine, the sole owner of s.st.
func (s *Session) handle(ctx context.Context, pdu smpp.PDU) bool {
	switch body := pdu.Body.(type) {
	case *smpp.BindTransmitter:
		s.handleBind(ctx, pdu.Sequence, BindTransmitter, &body.BindFields)
	case *smpp.BindReceiver:
		s.handleBind(ctx, pdu.Sequence, BindReceiver, &body.BindFields)
	case *smpp.BindTransceiver:
		s.handleBind(ctx, pdu.Sequence, BindTransceiver, &body.BindFields)
	case *smpp.SubmitSM:
		s.handleSubmit(ctx, pdu.Sequence, body)
	case *smpp.EnquireLink:
		s.reply(smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.EnquireLinkResp{}})
	case *smpp.Unbind:
		if s.cfg.OnUnbind != nil {
			s.cfg.OnUnbind(ctx)
		}
		s.reply(smpp.PDU{Sequence: pdu.Sequence, Body: &smpp.UnbindResp{}})
		s.st = stUnbound
		return true
	case *smpp.EnquireLinkResp, *smpp.DeliverSMResp:
		// Responses to our own server-initiated requests (enquire_link keep-alive is not sent
		// here, deliver_sm arrives in step-046): hand them to the waiting Send.
		s.route(pdu)
	default:
		// A known request that has no meaning for the server role (e.g. submit_sm_resp) or an
		// unsupported command: generic_nack with ESME_RINVCMDID.
		s.reply(smpp.PDU{Status: errs.StatusInvalidCmdID, Sequence: pdu.Sequence, Body: &smpp.GenericNACK{}})
	}
	return false
}

// handleBind runs the OnBind hook and answers with the matching bind_*_resp, transitioning to the
// bound state on success. A second bind on an already-bound session is rejected with ESME_RALYBND.
func (s *Session) handleBind(ctx context.Context, seq uint32, mode BindMode, f *smpp.BindFields) {
	if s.st.isBound() {
		s.reply(s.bindResp(mode, seq, errs.StatusAlreadyBound, s.cfg.SystemID))
		return
	}

	res := BindResult{Status: smpp.StatusOK, SystemID: s.cfg.SystemID}
	if s.cfg.OnBind != nil {
		res = s.cfg.OnBind(ctx, BindRequest{
			Mode:             mode,
			SystemID:         f.SystemID,
			Password:         f.Password,
			SystemType:       f.SystemType,
			InterfaceVersion: f.InterfaceVersion,
			AddrTON:          f.AddrTON,
			AddrNPI:          f.AddrNPI,
			AddressRange:     f.AddressRange,
		})
	}

	systemID := res.SystemID
	if systemID == "" {
		systemID = s.cfg.SystemID
	}
	s.reply(s.bindResp(mode, seq, res.Status, systemID))
	if res.Status == smpp.StatusOK {
		s.st = stateForMode(mode)
	}
}

// bindResp builds the bind_*_resp matching the request mode. On success it echoes the negotiated
// SMPP version in the sc_interface_version TLV (SMPP v3.4 §4.1.2).
func (s *Session) bindResp(mode BindMode, seq, status uint32, systemID string) smpp.PDU {
	fields := smpp.BindRespFields{SystemID: systemID}
	if status == smpp.StatusOK {
		fields.TLVs.Set(smpp.TagSCInterfaceVersion, []byte{smpp.InterfaceVersion34})
	}

	var body smpp.Body
	switch mode {
	case BindReceiver:
		body = &smpp.BindReceiverResp{BindRespFields: fields}
	case BindTransceiver:
		body = &smpp.BindTransceiverResp{BindRespFields: fields}
	default:
		body = &smpp.BindTransmitterResp{BindRespFields: fields}
	}
	return smpp.PDU{Status: status, Sequence: seq, Body: body}
}

// handleSubmit rejects an out-of-sequence submit_sm (before bind, or on a receiver bind) with
// ESME_RINVBNDSTS, otherwise runs OnSubmit and answers submit_sm_resp.
func (s *Session) handleSubmit(ctx context.Context, seq uint32, sm *smpp.SubmitSM) {
	if !s.st.canSubmit() {
		s.reply(smpp.PDU{Status: errs.StatusInvalidBindStatus, Sequence: seq, Body: &smpp.SubmitSMResp{}})
		return
	}

	// Wrap the body in a msg.Body at the first opportunity: from here it can be logged or traced
	// without leaking the plaintext (invariant a). The raw ShortMessage is never touched again.
	req := SubmitRequest{
		Source:             sm.SourceAddr,
		Destination:        sm.DestinationAddr,
		ServiceType:        sm.ServiceType,
		ESMClass:           sm.ESMClass,
		DataCoding:         sm.DataCoding,
		RegisteredDelivery: sm.RegisteredDelivery,
		Body:               msg.NewBody(sm.ShortMessage),
		TLVs:               sm.TLVs,
	}
	s.logger.Debug("submit_sm",
		"src", req.Source, "dst", req.Destination,
		"body", req.Body, "body_len", req.Body.Len())

	res := SubmitResult{Status: smpp.StatusOK}
	if s.cfg.OnSubmit != nil {
		res = s.cfg.OnSubmit(ctx, req)
	}

	out := &smpp.SubmitSMResp{}
	if res.Status == smpp.StatusOK {
		out.MessageID = res.MessageID
	}
	s.reply(smpp.PDU{Status: res.Status, Sequence: seq, Body: out})
}

// route hands a response to the Send waiting on its sequence_number. A response with no waiter
// (already timed out, or unsolicited) is dropped.
func (s *Session) route(pdu smpp.PDU) {
	s.mu.Lock()
	ch := s.pending[pdu.Sequence]
	s.mu.Unlock()
	if ch != nil {
		select {
		case ch <- pdu:
		default:
		}
	}
}

// nackStatus maps a decode error to the generic_nack command_status: an unknown command id is
// ESME_RINVCMDID, a malformed or oversized frame is ESME_RSYSERR.
func nackStatus(err error) uint32 {
	if errors.Is(err, smpp.ErrUnknownCommand) {
		return errs.StatusInvalidCmdID
	}
	return errs.StatusSysErr
}
