package smpp

// newBody allocates the body matching a command id, ready for unmarshal. An unsupported command
// id is rejected here, before any body bytes are read.
func newBody(id CommandID) (Body, error) {
	switch id {
	case CmdBindTransmitter:
		return &BindTransmitter{}, nil
	case CmdBindReceiver:
		return &BindReceiver{}, nil
	case CmdBindTransceiver:
		return &BindTransceiver{}, nil
	case CmdBindTransmitterResp:
		return &BindTransmitterResp{}, nil
	case CmdBindReceiverResp:
		return &BindReceiverResp{}, nil
	case CmdBindTransceiverResp:
		return &BindTransceiverResp{}, nil
	case CmdSubmitSM:
		return &SubmitSM{}, nil
	case CmdSubmitSMResp:
		return &SubmitSMResp{}, nil
	case CmdQuerySM:
		return &QuerySM{}, nil
	case CmdQuerySMResp:
		return &QuerySMResp{}, nil
	case CmdCancelSM:
		return &CancelSM{}, nil
	case CmdCancelSMResp:
		return &CancelSMResp{}, nil
	case CmdDeliverSM:
		return &DeliverSM{}, nil
	case CmdDeliverSMResp:
		return &DeliverSMResp{}, nil
	case CmdEnquireLink:
		return &EnquireLink{}, nil
	case CmdEnquireLinkResp:
		return &EnquireLinkResp{}, nil
	case CmdUnbind:
		return &Unbind{}, nil
	case CmdUnbindResp:
		return &UnbindResp{}, nil
	case CmdGenericNACK:
		return &GenericNACK{}, nil
	default:
		return nil, ErrUnknownCommand
	}
}

// BindFields is the shared body of the three bind requests (SMPP v3.4 §4.1.1); they differ only in
// command id.
type BindFields struct {
	SystemID         string
	Password         string
	SystemType       string
	InterfaceVersion uint8
	AddrTON          uint8
	AddrNPI          uint8
	AddressRange     string
}

func (b *BindFields) marshal(w *writer) {
	w.cOctetString(b.SystemID)
	w.cOctetString(b.Password)
	w.cOctetString(b.SystemType)
	w.byte(b.InterfaceVersion)
	w.byte(b.AddrTON)
	w.byte(b.AddrNPI)
	w.cOctetString(b.AddressRange)
}

func (b *BindFields) unmarshal(r *reader) error {
	b.SystemID = r.cOctetString(16)
	b.Password = r.cOctetString(9)
	b.SystemType = r.cOctetString(13)
	b.InterfaceVersion = r.byte()
	b.AddrTON = r.byte()
	b.AddrNPI = r.byte()
	b.AddressRange = r.cOctetString(41)
	return r.err
}

// BindTransmitter opens a transmit-only session (the ESME may submit_sm).
type BindTransmitter struct{ BindFields }

func (*BindTransmitter) commandID() CommandID { return CmdBindTransmitter }

// BindReceiver opens a receive-only session (the ESME receives deliver_sm).
type BindReceiver struct{ BindFields }

func (*BindReceiver) commandID() CommandID { return CmdBindReceiver }

// BindTransceiver opens a bidirectional session.
type BindTransceiver struct{ BindFields }

func (*BindTransceiver) commandID() CommandID { return CmdBindTransceiver }

// BindRespFields is the shared body of the three bind responses: the SMSC's system_id and optional
// TLVs (typically sc_interface_version).
type BindRespFields struct {
	SystemID string
	TLVs     TLVList
}

func (b *BindRespFields) marshal(w *writer) {
	w.cOctetString(b.SystemID)
	b.TLVs.marshal(w)
}

func (b *BindRespFields) unmarshal(r *reader) error {
	b.SystemID = r.cOctetString(16)
	b.TLVs = readTLVs(r)
	return r.err
}

// BindTransmitterResp answers a BindTransmitter.
type BindTransmitterResp struct{ BindRespFields }

func (*BindTransmitterResp) commandID() CommandID { return CmdBindTransmitterResp }

// BindReceiverResp answers a BindReceiver.
type BindReceiverResp struct{ BindRespFields }

func (*BindReceiverResp) commandID() CommandID { return CmdBindReceiverResp }

// BindTransceiverResp answers a BindTransceiver.
type BindTransceiverResp struct{ BindRespFields }

func (*BindTransceiverResp) commandID() CommandID { return CmdBindTransceiverResp }

// SMFields is the shared body of submit_sm and deliver_sm (SMPP v3.4 §4.4 / §4.6): identical wire
// layout, distinguished only by direction and command id.
type SMFields struct {
	ServiceType          string
	SourceAddrTON        uint8
	SourceAddrNPI        uint8
	SourceAddr           string
	DestAddrTON          uint8
	DestAddrNPI          uint8
	DestinationAddr      string
	ESMClass             uint8
	ProtocolID           uint8
	PriorityFlag         uint8
	ScheduleDeliveryTime string
	ValidityPeriod       string
	RegisteredDelivery   uint8
	ReplaceIfPresent     uint8
	DataCoding           uint8
	SMDefaultMsgID       uint8
	// ShortMessage is the raw user data, at most 254 octets. When it begins with a User Data Header
	// the ESMClassUDHIndicator bit is set (see udh.go). A larger body travels in a
	// TagMessagePayload TLV, with ShortMessage empty.
	ShortMessage []byte
	TLVs         TLVList
}

func (m *SMFields) marshal(w *writer) {
	if len(m.ShortMessage) > 254 {
		w.err = ErrMessageTooLong
		return
	}
	w.cOctetString(m.ServiceType)
	w.byte(m.SourceAddrTON)
	w.byte(m.SourceAddrNPI)
	w.cOctetString(m.SourceAddr)
	w.byte(m.DestAddrTON)
	w.byte(m.DestAddrNPI)
	w.cOctetString(m.DestinationAddr)
	w.byte(m.ESMClass)
	w.byte(m.ProtocolID)
	w.byte(m.PriorityFlag)
	w.cOctetString(m.ScheduleDeliveryTime)
	w.cOctetString(m.ValidityPeriod)
	w.byte(m.RegisteredDelivery)
	w.byte(m.ReplaceIfPresent)
	w.byte(m.DataCoding)
	w.byte(m.SMDefaultMsgID)
	w.byte(uint8(len(m.ShortMessage))) //nolint:gosec // length bounded to <=254 by the guard above
	w.octets(m.ShortMessage)
	m.TLVs.marshal(w)
}

func (m *SMFields) unmarshal(r *reader) error {
	m.ServiceType = r.cOctetString(6)
	m.SourceAddrTON = r.byte()
	m.SourceAddrNPI = r.byte()
	m.SourceAddr = r.cOctetString(21)
	m.DestAddrTON = r.byte()
	m.DestAddrNPI = r.byte()
	m.DestinationAddr = r.cOctetString(21)
	m.ESMClass = r.byte()
	m.ProtocolID = r.byte()
	m.PriorityFlag = r.byte()
	m.ScheduleDeliveryTime = r.cOctetString(17)
	m.ValidityPeriod = r.cOctetString(17)
	m.RegisteredDelivery = r.byte()
	m.ReplaceIfPresent = r.byte()
	m.DataCoding = r.byte()
	m.SMDefaultMsgID = r.byte()
	smLength := int(r.byte())
	m.ShortMessage = r.octetString(smLength)
	m.TLVs = readTLVs(r)
	return r.err
}

// SubmitSM carries a mobile-terminated message from the ESME to the SMSC.
type SubmitSM struct{ SMFields }

func (*SubmitSM) commandID() CommandID { return CmdSubmitSM }

// DeliverSM carries a mobile-originated message or a delivery receipt from the SMSC to the ESME;
// the ESMClassMCDeliveryReceipt bit tells them apart (M4).
type DeliverSM struct{ SMFields }

func (*DeliverSM) commandID() CommandID { return CmdDeliverSM }

// MessageIDResp is the shared body of submit_sm_resp and deliver_sm_resp: the assigned message id
// (empty for deliver_sm_resp) and optional TLVs.
type MessageIDResp struct {
	MessageID string
	TLVs      TLVList
}

func (m *MessageIDResp) marshal(w *writer) {
	w.cOctetString(m.MessageID)
	m.TLVs.marshal(w)
}

func (m *MessageIDResp) unmarshal(r *reader) error {
	m.MessageID = r.cOctetString(65)
	m.TLVs = readTLVs(r)
	return r.err
}

// SubmitSMResp returns the SMSC-assigned message id for a submit_sm.
type SubmitSMResp struct{ MessageIDResp }

func (*SubmitSMResp) commandID() CommandID { return CmdSubmitSMResp }

// DeliverSMResp acknowledges a deliver_sm; its message id is conventionally empty.
type DeliverSMResp struct{ MessageIDResp }

func (*DeliverSMResp) commandID() CommandID { return CmdDeliverSMResp }

// QuerySM asks the SMSC for the state of a previously submitted message (SMPP v3.4 §4.8.1), keyed by
// its message_id scoped to the original source address.
type QuerySM struct {
	MessageID     string
	SourceAddrTON uint8
	SourceAddrNPI uint8
	SourceAddr    string
}

func (*QuerySM) commandID() CommandID { return CmdQuerySM }

func (q *QuerySM) marshal(w *writer) {
	w.cOctetString(q.MessageID)
	w.byte(q.SourceAddrTON)
	w.byte(q.SourceAddrNPI)
	w.cOctetString(q.SourceAddr)
}

func (q *QuerySM) unmarshal(r *reader) error {
	q.MessageID = r.cOctetString(65)
	q.SourceAddrTON = r.byte()
	q.SourceAddrNPI = r.byte()
	q.SourceAddr = r.cOctetString(21)
	return r.err
}

// QuerySMResp returns the queried message's state (SMPP v3.4 §4.8.2): the message_id echoed, the
// final_date (empty when the message is not yet in a final state), the message_state and a network
// error_code.
type QuerySMResp struct {
	MessageID    string
	FinalDate    string
	MessageState uint8
	ErrorCode    uint8
}

func (*QuerySMResp) commandID() CommandID { return CmdQuerySMResp }

func (q *QuerySMResp) marshal(w *writer) {
	w.cOctetString(q.MessageID)
	w.cOctetString(q.FinalDate)
	w.byte(q.MessageState)
	w.byte(q.ErrorCode)
}

func (q *QuerySMResp) unmarshal(r *reader) error {
	q.MessageID = r.cOctetString(65)
	q.FinalDate = r.cOctetString(17)
	q.MessageState = r.byte()
	q.ErrorCode = r.byte()
	return r.err
}

// CancelSM requests cancellation of one previously submitted message by message_id, or of a batch by
// source/destination address when message_id is empty (SMPP v3.4 §4.9.1).
type CancelSM struct {
	ServiceType     string
	MessageID       string
	SourceAddrTON   uint8
	SourceAddrNPI   uint8
	SourceAddr      string
	DestAddrTON     uint8
	DestAddrNPI     uint8
	DestinationAddr string
}

func (*CancelSM) commandID() CommandID { return CmdCancelSM }

func (c *CancelSM) marshal(w *writer) {
	w.cOctetString(c.ServiceType)
	w.cOctetString(c.MessageID)
	w.byte(c.SourceAddrTON)
	w.byte(c.SourceAddrNPI)
	w.cOctetString(c.SourceAddr)
	w.byte(c.DestAddrTON)
	w.byte(c.DestAddrNPI)
	w.cOctetString(c.DestinationAddr)
}

func (c *CancelSM) unmarshal(r *reader) error {
	c.ServiceType = r.cOctetString(6)
	c.MessageID = r.cOctetString(65)
	c.SourceAddrTON = r.byte()
	c.SourceAddrNPI = r.byte()
	c.SourceAddr = r.cOctetString(21)
	c.DestAddrTON = r.byte()
	c.DestAddrNPI = r.byte()
	c.DestinationAddr = r.cOctetString(21)
	return r.err
}

// CancelSMResp acknowledges a cancel_sm; it has no body (SMPP v3.4 §4.9.2).
type CancelSMResp struct{}

func (*CancelSMResp) commandID() CommandID    { return CmdCancelSMResp }
func (*CancelSMResp) marshal(*writer)         {}
func (*CancelSMResp) unmarshal(*reader) error { return nil }

// EnquireLink is the keep-alive probe (SMPP v3.4 §4.11); it has no body.
type EnquireLink struct{}

func (*EnquireLink) commandID() CommandID    { return CmdEnquireLink }
func (*EnquireLink) marshal(*writer)         {}
func (*EnquireLink) unmarshal(*reader) error { return nil }

// EnquireLinkResp answers an EnquireLink; it has no body.
type EnquireLinkResp struct{}

func (*EnquireLinkResp) commandID() CommandID    { return CmdEnquireLinkResp }
func (*EnquireLinkResp) marshal(*writer)         {}
func (*EnquireLinkResp) unmarshal(*reader) error { return nil }

// Unbind requests an orderly session close (SMPP v3.4 §4.2); it has no body.
type Unbind struct{}

func (*Unbind) commandID() CommandID    { return CmdUnbind }
func (*Unbind) marshal(*writer)         {}
func (*Unbind) unmarshal(*reader) error { return nil }

// UnbindResp answers an Unbind; it has no body.
type UnbindResp struct{}

func (*UnbindResp) commandID() CommandID    { return CmdUnbindResp }
func (*UnbindResp) marshal(*writer)         {}
func (*UnbindResp) unmarshal(*reader) error { return nil }

// GenericNACK reports a PDU that could not be decoded or is unsupported; the reason is in the
// PDU's command_status. It has no body.
type GenericNACK struct{}

func (*GenericNACK) commandID() CommandID    { return CmdGenericNACK }
func (*GenericNACK) marshal(*writer)         {}
func (*GenericNACK) unmarshal(*reader) error { return nil }
