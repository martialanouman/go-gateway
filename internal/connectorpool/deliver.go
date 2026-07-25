package connectorpool

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// handleDeliver is the return-path handler the bind's worker pool calls for each deliver_sm, BEFORE it
// acknowledges. It classifies the PDU (a receipt vs a mobile-originated message) by the esm_class MC
// Delivery Receipt bit and publishes it durably: a DLR to dlr.events, an MO to mo.inbound. Returning
// an error makes the bind withhold the deliver_sm_resp, so the SMSC retries — no MO/DLR is lost, at
// the cost of an occasional duplicate the consumers dedup. It never logs the MO body (invariant a).
func (s *Service) handleDeliver(ctx context.Context, ds *smpp.DeliverSM) error {
	receivedAt := time.Now().UTC()
	if ds.ESMClass&smpp.ESMClassMCDeliveryReceipt != 0 {
		rec, err := pipeline.EncodeDLR(s.buildDLR(ds, receivedAt))
		if err != nil {
			return fmt.Errorf("connectorpool: encode dlr.events: %w", err)
		}
		return s.deps.Producer.Produce(ctx, rec)
	}
	rec, err := pipeline.EncodeMO(s.buildMO(ds, receivedAt))
	if err != nil {
		return fmt.Errorf("connectorpool: encode mo.inbound: %w", err)
	}
	return s.deps.Producer.Produce(ctx, rec)
}

// buildMO projects a mobile-originated deliver_sm onto an MOInbound. The content travels in the masked
// Body; a payload larger than short_message arrives in the message_payload TLV.
func (s *Service) buildMO(ds *smpp.DeliverSM, receivedAt time.Time) pipeline.MOInbound {
	body := ds.ShortMessage
	if len(body) == 0 {
		if payload, ok := ds.TLVs.Get(smpp.TagMessagePayload); ok {
			body = payload
		}
	}
	return pipeline.MOInbound{
		ConnectorID: s.deps.ConnectorID,
		From:        ds.SourceAddr,
		To:          ds.DestinationAddr,
		Body:        msg.NewBody(body),
		DataCoding:  ds.DataCoding,
		ESMClass:    ds.ESMClass,
		ReceivedAt:  receivedAt,
	}
}

// buildDLR projects a delivery-receipt deliver_sm onto a DLREvent. The SMSC message id and state come
// from the receipt TLVs when present (authoritative), falling back to the textual receipt body.
func (s *Service) buildDLR(ds *smpp.DeliverSM, receivedAt time.Time) pipeline.DLREvent {
	r := parseReceipt(ds)
	return pipeline.DLREvent{
		ConnectorID:   s.deps.ConnectorID,
		SMSCMessageID: r.smscMsgID,
		State:         r.state,
		Stat:          r.stat,
		ErrorCode:     r.errCode,
		SubmitDate:    r.submitDate,
		DoneDate:      r.doneDate,
		ReceivedAt:    receivedAt,
	}
}

// receipt holds the fields extracted from a delivery receipt.
type receipt struct {
	smscMsgID  string
	state      uint8
	stat       string
	errCode    string
	submitDate string
	doneDate   string
}

// parseReceipt reads a delivery receipt from a deliver_sm. The receipted message id and message state
// come from the TLVs (receipted_message_id 0x001E, message_state 0x0427) when present — they are the
// reliable, structured source — and the textual receipt body fills the rest (stat, err, dates). The
// SMSC message id falls back to the text "id:" only when the TLV is absent.
func parseReceipt(ds *smpp.DeliverSM) receipt {
	var r receipt
	if v, ok := ds.TLVs.Get(smpp.TagReceiptedMessageID); ok {
		r.smscMsgID = string(v)
	}
	if v, ok := ds.TLVs.Get(smpp.TagMessageState); ok && len(v) == 1 {
		r.state = v[0]
	}

	fields := parseReceiptText(string(ds.ShortMessage))
	if r.smscMsgID == "" {
		r.smscMsgID = fields["id"]
	}
	r.stat = fields["stat"]
	r.errCode = fields["err"]
	r.submitDate = fields["submit date"]
	r.doneDate = fields["done date"]
	return r
}

// parseReceiptText extracts the space-delimited key:value fields of an SMPP v3.4 delivery-receipt
// body ("id:.. sub:.. dlvrd:.. submit date:.. done date:.. stat:.. err:.. text:.."). It STOPS at the
// "text:" field, which echoes up to the first 20 characters of the ORIGINAL message and must never be
// captured or stored (invariant a). The two-word "submit date" / "done date" keys are recognised.
func parseReceiptText(s string) map[string]string {
	// Drop the body-echoing tail before anything else.
	if i := strings.Index(s, "text:"); i >= 0 {
		s = s[:i]
	}
	// Fold the two-word date keys so a plain whitespace split yields clean key:value tokens.
	s = strings.ReplaceAll(s, "submit date:", "submit_date:")
	s = strings.ReplaceAll(s, "done date:", "done_date:")

	out := make(map[string]string)
	for _, tok := range strings.Fields(s) {
		k, v, ok := strings.Cut(tok, ":")
		if !ok {
			continue
		}
		switch k {
		case "submit_date":
			out["submit date"] = v
		case "done_date":
			out["done date"] = v
		default:
			out[k] = v
		}
	}
	return out
}
