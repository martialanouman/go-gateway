package smppserver

import (
	"context"

	"github.com/martialanouman/go-gateway/internal/pipeline"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/platform/uuidx"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// onSubmit returns the session's submit_sm hook, bound to this connection's bind identity. It maps
// the PDU to a pipeline.InboundMT and hands it to the shared ingestor — the exact path a REST submit
// takes — so a message reaches mt.inbound identically whichever protocol carried it (protocol
// parity). A nil ingestor (the bind-only tests) rejects with ESME_RSUBMITFAIL.
//
// The body never leaves req.Body except inside the audited WIRE codec: it is carried as a masking
// msg.Body straight into the envelope and never logged or spanned here (invariant a).
func (l *Listener) onSubmit(_ context.Context, st *connState) session.SubmitHandler {
	return func(sctx context.Context, req session.SubmitRequest) session.SubmitResult {
		if l.ingestor == nil {
			return session.SubmitResult{Status: errs.StatusSubmitFail}
		}

		sctx, span := l.opts.Tracer.Start(sctx, "smpp.submit")
		defer span.End()

		messageID := uuidx.New()
		traceID := uuidx.New()
		dataCoding := int(req.DataCoding)

		// accountID/customerID were resolved to typed uuids at bind, so the hot path does no parsing.
		env := pipeline.InboundMT{
			MessageID:  messageID,
			TraceID:    traceID,
			AccountID:  st.accountID,
			CustomerID: st.customerID,
			From:       req.Source,
			To:         req.Destination,
			Body:       submitBody(req), // already a masking msg.Body; never revealed here (invariant a)
			// The router resolves the wire encoding; an SMPP submit carries data_coding instead of the
			// REST encoding enum, so we pass "auto" and let data_coding drive downstream.
			Encoding:           "auto",
			ESMClass:           req.ESMClass,
			RegisteredDelivery: req.RegisteredDelivery&smpp.RegisteredDeliveryReceiptMask != 0,
			DataCoding:         &dataCoding,
			SubmittedAt:        l.opts.Now(),
		}

		if err := l.ingestor.Accept(sctx, env); err != nil {
			status := errs.SMPPStatusForError(err)
			l.logger.ErrorContext(sctx, "smpp submit: ingest failed",
				"message_id", messageID, "account_id", st.accountID, "command_status", status)
			return session.SubmitResult{Status: status}
		}
		return session.SubmitResult{Status: smpp.StatusOK, MessageID: messageID.String()}
	}
}

// submitBody resolves the message body of a submit_sm. A body larger than the 254-octet short_message
// field travels in the message_payload TLV with short_message empty (SMPP v3.4 §5.2.30); in that case
// the TLV carries the content. The result stays a masking msg.Body, so the plaintext never escapes
// into a log or span here (invariant a).
func submitBody(req session.SubmitRequest) msg.Body {
	if req.Body.IsEmpty() {
		if payload, ok := req.TLVs.Get(smpp.TagMessagePayload); ok {
			return msg.NewBody(payload)
		}
	}
	return req.Body
}
