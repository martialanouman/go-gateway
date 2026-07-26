package modlrrouter

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/martialanouman/go-gateway/internal/dlrmap"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// moDeliverSM encodes a mobile-originated deliver_sm for an account's bind. The revealed body goes on
// the SMPP wire (or the message_payload TLV when long) — an audited egress, never a log (invariant a).
// The sequence_number is a placeholder: the owning pod's send window allocates its own.
func moDeliverSM(mo pipeline.MORouted) ([]byte, error) {
	ds := &smpp.DeliverSM{}
	ds.SourceAddr = mo.From
	ds.DestinationAddr = mo.To
	ds.DataCoding = mo.DataCoding
	ds.ESMClass = mo.ESMClass
	body := mo.Body.Reveal() // audited: MO body -> deliver_sm wire, never logged
	if len(body) > 254 {
		ds.TLVs.Set(smpp.TagMessagePayload, body)
	} else {
		ds.ShortMessage = body
	}
	return smpp.Marshal(smpp.PDU{Sequence: 1, Body: ds})
}

// dlrDeliverSM encodes a delivery-receipt deliver_sm for an account's bind. The receipt references the
// account's own message id (what it received in its submit_sm_resp), so it correlates the report to
// its message. A receipt carries no message body.
func dlrDeliverSM(m dlrmap.Mapping, dlr pipeline.DLREvent) ([]byte, error) {
	ds := &smpp.DeliverSM{}
	ds.ESMClass = smpp.ESMClassMCDeliveryReceipt
	// A receipt's source is the MSISDN the original MT was sent to; its destination is the original
	// sender id.
	ds.SourceAddr = m.DestAddr
	ds.DestinationAddr = m.SourceAddr
	ds.ShortMessage = []byte(fmt.Sprintf("id:%s stat:%s err:%s", m.MessageID, dlr.Stat, dlr.ErrorCode))
	ds.TLVs.Set(smpp.TagReceiptedMessageID, []byte(m.MessageID.String()))
	ds.TLVs.Set(smpp.TagMessageState, []byte{dlr.State})
	return smpp.Marshal(smpp.PDU{Sequence: 1, Body: ds})
}

// moWebhookPayload is the JSON body POSTed to an MO webhook. Body is base64 (a []byte field), so binary
// (UCS-2) content survives. This payload legitimately carries the body — the recipient is the account's
// own endpoint — but it is never logged (the sender guarantees invariant a).
type moWebhookPayload struct {
	EventID    string    `json:"event_id"`
	MessageID  string    `json:"message_id"`
	AccountID  string    `json:"account_id"`
	From       string    `json:"from"`
	To         string    `json:"to"`
	Body       []byte    `json:"body"`
	Encoding   string    `json:"encoding"`
	ReceivedAt time.Time `json:"received_at"`
}

// moWebhookBody marshals the MO webhook payload. The event id is the message id, stable so the receiver
// dedups a redelivery.
func moWebhookBody(mo pipeline.MORouted) (id string, payload []byte, err error) {
	id = mo.MessageID.String()
	payload, err = json.Marshal(moWebhookPayload{
		EventID:    id,
		MessageID:  id,
		AccountID:  mo.AccountID.String(),
		From:       mo.From,
		To:         mo.To,
		Body:       mo.Body.Reveal(), // audited: body -> durable webhook payload, never logged
		Encoding:   mo.Encoding,
		ReceivedAt: mo.ReceivedAt,
	})
	return id, payload, err
}

// dlrWebhookPayload is the JSON body POSTed to a DLR webhook. A receipt carries no message body.
type dlrWebhookPayload struct {
	EventID    string    `json:"event_id"`
	MessageID  string    `json:"message_id"`
	AccountID  string    `json:"account_id"`
	Status     string    `json:"status"`
	State      uint8     `json:"state"`
	ErrorCode  string    `json:"error_code"`
	ReceivedAt time.Time `json:"received_at"`
}

// dlrWebhookBody marshals the DLR webhook payload. The event id includes the state so distinct states
// for one message are distinct events the receiver dedups.
func dlrWebhookBody(m dlrmap.Mapping, dlr pipeline.DLREvent) (id string, payload []byte, err error) {
	id = fmt.Sprintf("%s:%d", m.MessageID, dlr.State)
	payload, err = json.Marshal(dlrWebhookPayload{
		EventID:    id,
		MessageID:  m.MessageID.String(),
		AccountID:  m.AccountID.String(),
		Status:     dlr.Stat,
		State:      dlr.State,
		ErrorCode:  dlr.ErrorCode,
		ReceivedAt: dlr.ReceivedAt,
	})
	return id, payload, err
}
