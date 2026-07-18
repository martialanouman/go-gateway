package fakesmsc

import (
	"fmt"

	"github.com/martialanouman/go-gateway/internal/smpp"
)

// DLRState is the final state a delivery receipt reports. It maps to the SMPP message_state value
// (§5.2.28) and the textual "stat:" field of the receipt body. M2 does not consume DLRs (that is
// M4); this exists so the fake SMSC can drive the return path once mo-dlr-router-svc lands.
type DLRState struct {
	code uint8
	text string
}

// The delivery-receipt final states used by tests.
var (
	// Delivered reports a successful delivery (message_state DELIVERED).
	Delivered = DLRState{code: 2, text: "DELIVRD"}
	// Expired reports that the message expired before delivery.
	Expired = DLRState{code: 3, text: "EXPIRED"}
	// Undeliverable reports a permanent delivery failure.
	Undeliverable = DLRState{code: 5, text: "UNDELIV"}
)

// SendDLR emits a deliver_sm carrying a delivery receipt for smscMsgID to every connection bound as
// a receiver or transceiver. It correlates by the SMSC message id the fake assigned in a prior
// submit_sm_resp (§1.11). It returns an error only if no bound receiver is available or a write
// fails; the receipt is best-effort, matching a real SMSC.
func (s *Server) SendDLR(smscMsgID string, state DLRState) error {
	body := &smpp.DeliverSM{}
	body.ESMClass = smpp.ESMClassMCDeliveryReceipt
	body.ShortMessage = []byte(fmt.Sprintf("id:%s stat:%s", smscMsgID, state.text))
	body.TLVs.Set(smpp.TagReceiptedMessageID, []byte(smscMsgID))
	body.TLVs.Set(smpp.TagMessageState, []byte{state.code})

	pdu := smpp.PDU{Sequence: s.seq.Add(1), Body: body}
	return s.sendToReceivers(pdu)
}

// sendToReceivers writes pdu to every connection eligible to receive deliver_sm.
func (s *Server) sendToReceivers(pdu smpp.PDU) error {
	s.mu.Lock()
	targets := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		if c.canRecv {
			targets = append(targets, c)
		}
	}
	s.mu.Unlock()

	if len(targets) == 0 {
		return fmt.Errorf("fakesmsc: no bound receiver to deliver to")
	}
	for _, c := range targets {
		if err := c.write(pdu); err != nil {
			return fmt.Errorf("fakesmsc: send deliver_sm: %w", err)
		}
	}
	return nil
}
