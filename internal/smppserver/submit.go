package smppserver

import (
	"context"

	"github.com/martialanouman/go-gateway/internal/observability"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/encoding"
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
	return func(sctx context.Context, req session.SubmitRequest) (res session.SubmitResult) {
		if l.ingestor == nil {
			return session.SubmitResult{Status: errs.StatusSubmitFail}
		}

		sctx, span := l.opts.Tracer.Start(sctx, "smpp.submit")
		defer span.End()
		// The SMPP ingress root span. The outcome is a command_status, not an error, so it is translated
		// back to the flat code; 0 is ESME_ROK. Unmarked, the ratio drops this span and every pipeline
		// span below it loses its parent (step-181).
		defer func() {
			if res.Status != 0 {
				observability.RecordSpanError(span, errs.CodeFromSMPPStatus(res.Status))
			}
		}()

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
			// An SMPP submit expresses its coding through data_coding, not the REST encoding enum, so we
			// resolve Encoding from data_coding (the whole pipeline keys the CDR and wire on Encoding) and
			// still carry the raw data_coding as the client override the connector honours.
			Encoding:           encoding.FromDataCoding(req.DataCoding),
			ESMClass:           req.ESMClass,
			RegisteredDelivery: req.RegisteredDelivery&smpp.RegisteredDeliveryReceiptMask != 0,
			ValidityPeriod:     optionalString(req.ValidityPeriod),
			Priority:           int(req.PriorityFlag),
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

// optionalString maps an SMPP C-Octet String field to the envelope's optional pointer: an empty
// field (the ESME left it default) is nil, any value is carried.
func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
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
