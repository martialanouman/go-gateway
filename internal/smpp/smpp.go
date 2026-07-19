// Package smpp is a pure SMPP v3.4 PDU codec: it encodes and decodes the protocol data units
// the gateway exchanges with an SMSC, and nothing else. It has no dependency on the pipeline or
// on storage (guide de codage §9): the codec is a leaf, so the outbound connector, the future
// inbound server (M3) and the in-repo fake SMSC can all share one implementation of the wire
// format.
//
// The decoder is a security surface (strategie-de-test §4.2): it parses bytes that arrive from a
// remote peer, so it must never panic on malformed input. Every read is bounds-checked and the
// first error short-circuits the rest (see codec.go); a fuzz test (FuzzUnmarshal) guards this.
//
// Scope is the outbound leg plus the inbound server operations: bind_transmitter/receiver/
// transceiver (+resp), submit_sm (+resp), deliver_sm (+resp), query_sm (+resp), cancel_sm (+resp),
// enquire_link (+resp) and unbind (+resp), plus generic_nack. TLV and UDH are supported, and
// payloads larger than 254 octets travel in the message_payload TLV. replace_sm and data_sm are out
// of scope and unsupported (specification §5.1).
package smpp

import "errors"

// CommandID identifies a PDU. Request command ids have the high bit clear; the matching response
// sets it (id | 0x80000000).
type CommandID uint32

// The command ids this codec understands (SMPP v3.4 §5.1.2.1). Command ids outside this set are
// rejected on decode with ErrUnknownCommand.
const (
	CmdGenericNACK         CommandID = 0x80000000
	CmdBindReceiver        CommandID = 0x00000001
	CmdBindReceiverResp    CommandID = 0x80000001
	CmdBindTransmitter     CommandID = 0x00000002
	CmdBindTransmitterResp CommandID = 0x80000002
	CmdQuerySM             CommandID = 0x00000003
	CmdQuerySMResp         CommandID = 0x80000003
	CmdSubmitSM            CommandID = 0x00000004
	CmdSubmitSMResp        CommandID = 0x80000004
	CmdDeliverSM           CommandID = 0x00000005
	CmdDeliverSMResp       CommandID = 0x80000005
	CmdUnbind              CommandID = 0x00000006
	CmdUnbindResp          CommandID = 0x80000006
	CmdCancelSM            CommandID = 0x00000008
	CmdCancelSMResp        CommandID = 0x80000008
	CmdBindTransceiver     CommandID = 0x00000009
	CmdBindTransceiverResp CommandID = 0x80000009
	CmdEnquireLink         CommandID = 0x00000015
	CmdEnquireLinkResp     CommandID = 0x80000015
)

// StatusOK is command_status ESME_ROK: success, and the only status a request ever carries. The
// error command_status values live in internal/platform/errors, which owns the mapping between a
// gateway Code and its SMPP status; the codec treats command_status as an opaque uint32 so it
// stays free of the business catalogue.
const StatusOK uint32 = 0x00000000

// InterfaceVersion34 is the interface_version byte for SMPP v3.4, sent in a bind and echoed in the
// sc_interface_version TLV of a bind response.
const InterfaceVersion34 uint8 = 0x34

// esm_class bits (SMPP v3.4 §5.2.12). Only the ones the pipeline needs are named.
const (
	// ESMClassDefault is a plain point-to-point message with no special features.
	ESMClassDefault uint8 = 0x00
	// ESMClassUDHIndicator marks that short_message begins with a User Data Header (see udh.go).
	ESMClassUDHIndicator uint8 = 0x40
	// ESMClassMCDeliveryReceipt marks a deliver_sm that carries a delivery receipt (DLR) rather
	// than a mobile-originated message; MO vs DLR is distinguished by this bit (M4).
	ESMClassMCDeliveryReceipt uint8 = 0x04
)

// data_coding values (SMPP v3.4 §5.2.19 / GSM 03.38). The pipeline maps its Encoding enum onto
// these.
const (
	DataCodingGSM7   uint8 = 0x00
	DataCodingBinary uint8 = 0x04
	DataCodingUCS2   uint8 = 0x08
)

// RegisteredDeliveryReceipt is the registered_delivery bit (SMPP v3.4 §5.2.17): request a
// delivery receipt on final state.
const RegisteredDeliveryReceipt uint8 = 0x01

// Address type-of-number / numbering-plan-indicator values the gateway uses by default
// (SMPP v3.4 §5.2.5–5.2.6).
const (
	TONInternational uint8 = 0x01
	TONAlphanumeric  uint8 = 0x05
	NPIUnknown       uint8 = 0x00
	NPIISDN          uint8 = 0x01
)

// Codec error sentinels. All decode failures are one of these (or an io error from ReadPDU): a
// decode never panics.
var (
	// ErrShortHeader is returned when a frame is shorter than the 16-octet header.
	ErrShortHeader = errors.New("smpp: pdu shorter than header")
	// ErrLengthMismatch is returned when command_length does not equal the frame size.
	ErrLengthMismatch = errors.New("smpp: command_length does not match frame size")
	// ErrMalformedBody is returned when a body is truncated, unterminated or carries trailing bytes.
	ErrMalformedBody = errors.New("smpp: malformed pdu body")
	// ErrUnknownCommand is returned when a frame carries a command id this codec does not support.
	ErrUnknownCommand = errors.New("smpp: unknown command id")
	// ErrMessageTooLong is returned by Marshal when short_message exceeds 254 octets; a payload
	// that large must travel in the message_payload TLV instead (specification §5.1).
	ErrMessageTooLong = errors.New("smpp: short_message exceeds 254 octets; use message_payload TLV")
	// ErrTLVTooLong is returned by Marshal when a TLV value exceeds the 65535-octet length field.
	ErrTLVTooLong = errors.New("smpp: tlv value exceeds 65535 octets")
	// ErrPDUTooLarge is returned by ReadPDU when command_length exceeds maxPDULen, so a hostile or
	// corrupt length field cannot force a huge allocation.
	ErrPDUTooLarge = errors.New("smpp: pdu length exceeds maximum")
)
