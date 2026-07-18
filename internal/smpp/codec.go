package smpp

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// headerLen is the fixed SMPP PDU header: command_length, command_id, command_status,
	// sequence_number, each a 4-octet big-endian integer.
	headerLen = 16
	// maxPDULen caps how large a framed PDU ReadPDU will allocate for. A message_payload TLV can
	// legitimately carry a few KiB; 1 MiB is far above any real PDU and well below a memory risk,
	// so a corrupt or hostile command_length cannot force a large allocation.
	maxPDULen = 1 << 20
)

// PDU is one decoded protocol data unit: the per-frame header fields that vary independently of
// the body (command_status and sequence_number) plus the typed body. The command id is derived
// from the body, so a PDU can never carry a header id that disagrees with its body type.
type PDU struct {
	// Status is the command_status. It is StatusOK on every request and on a successful response,
	// and an ESME_* value on a failed response.
	Status uint32
	// Sequence is the sequence_number correlating a request with its response within a bind.
	Sequence uint32
	// Body is the typed payload; its concrete type determines CommandID.
	Body Body
}

// CommandID reports the command id of the PDU, taken from its body.
func (p PDU) CommandID() CommandID { return p.Body.commandID() }

// Body is a typed PDU payload. Implementations are the concrete PDU structs in pdu.go. marshal and
// unmarshal are unexported because encoding goes through Marshal/Unmarshal, which frame the header.
type Body interface {
	commandID() CommandID
	marshal(w *writer)
	unmarshal(r *reader) error
}

// Marshal encodes a PDU to its wire form, computing command_length from the encoded size.
func Marshal(p PDU) ([]byte, error) {
	if p.Body == nil {
		return nil, fmt.Errorf("smpp: marshal: nil body")
	}
	w := &writer{}
	w.u32(0) // command_length placeholder, back-filled below
	w.u32(uint32(p.Body.commandID()))
	w.u32(p.Status)
	w.u32(p.Sequence)
	p.Body.marshal(w)
	if w.err != nil {
		return nil, w.err
	}
	//nolint:gosec // a marshalled PDU is bounded well below 4 GiB (TLV values are <=64 KiB)
	binary.BigEndian.PutUint32(w.buf[0:4], uint32(len(w.buf)))
	return w.buf, nil
}

// Unmarshal decodes exactly one framed PDU. data must be a single complete frame: its length must
// equal the command_length in the header. It never panics on malformed input.
func Unmarshal(data []byte) (PDU, error) {
	if len(data) < headerLen {
		return PDU{}, ErrShortHeader
	}
	length := binary.BigEndian.Uint32(data[0:4])
	if int(length) != len(data) {
		return PDU{}, ErrLengthMismatch
	}
	id := CommandID(binary.BigEndian.Uint32(data[4:8]))
	body, err := newBody(id)
	if err != nil {
		return PDU{}, err
	}
	r := &reader{buf: data[headerLen:]}
	if err := body.unmarshal(r); err != nil {
		return PDU{}, err
	}
	if r.err != nil {
		return PDU{}, r.err
	}
	// A well-formed body consumes its whole frame; leftover octets mean the frame is malformed.
	if r.remaining() != 0 {
		return PDU{}, ErrMalformedBody
	}
	return PDU{
		Status:   binary.BigEndian.Uint32(data[8:12]),
		Sequence: binary.BigEndian.Uint32(data[12:16]),
		Body:     body,
	}, nil
}

// ReadPDU reads one framed PDU from r, blocking until a whole frame arrives. It reads the 4-octet
// command_length first, bounds-checks it against maxPDULen, then reads the remainder. An io error
// (including io.EOF on a closed connection) is returned as-is so a caller can distinguish a clean
// peer close from a protocol fault.
func ReadPDU(r io.Reader) (PDU, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return PDU{}, err
	}
	length := binary.BigEndian.Uint32(head[:])
	if length < headerLen {
		return PDU{}, ErrLengthMismatch
	}
	if length > maxPDULen {
		return PDU{}, ErrPDUTooLarge
	}
	data := make([]byte, length)
	copy(data[0:4], head[:])
	if _, err := io.ReadFull(r, data[4:]); err != nil {
		return PDU{}, err
	}
	return Unmarshal(data)
}

// WritePDU marshals p and writes it to w in a single Write.
func WritePDU(w io.Writer, p PDU) error {
	b, err := Marshal(p)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// reader consumes a body slice, tracking the first error. Once err is set every read is a no-op
// returning a zero value, so a truncated or malformed frame fails cleanly instead of panicking on
// an out-of-range slice (guide de codage §13).
type reader struct {
	buf []byte
	pos int
	err error
}

func (r *reader) remaining() int { return len(r.buf) - r.pos }

func (r *reader) byte() uint8 {
	if r.err != nil {
		return 0
	}
	if r.pos+1 > len(r.buf) {
		r.err = ErrMalformedBody
		return 0
	}
	b := r.buf[r.pos]
	r.pos++
	return b
}

func (r *reader) u16() uint16 {
	if r.err != nil {
		return 0
	}
	if r.pos+2 > len(r.buf) {
		r.err = ErrMalformedBody
		return 0
	}
	v := binary.BigEndian.Uint16(r.buf[r.pos : r.pos+2])
	r.pos += 2
	return v
}

// cOctetString reads a NUL-terminated string. max bounds the search (including the NUL) so a
// missing terminator cannot scan the whole buffer; max <= 0 means scan to the end of the body.
func (r *reader) cOctetString(max int) string {
	if r.err != nil {
		return ""
	}
	limit := len(r.buf)
	if max > 0 && r.pos+max < limit {
		limit = r.pos + max
	}
	for i := r.pos; i < limit; i++ {
		if r.buf[i] == 0 {
			s := string(r.buf[r.pos:i])
			r.pos = i + 1
			return s
		}
	}
	r.err = ErrMalformedBody
	return ""
}

// octetString reads exactly n octets, returning a copy so the result may outlive the input buffer.
func (r *reader) octetString(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || r.pos+n > len(r.buf) {
		r.err = ErrMalformedBody
		return nil
	}
	out := make([]byte, n)
	copy(out, r.buf[r.pos:r.pos+n])
	r.pos += n
	return out
}

// writer accumulates encoded octets. It carries an err field for symmetry with reader and to
// surface an over-long short_message from a body's marshal.
type writer struct {
	buf []byte
	err error
}

func (w *writer) byte(b uint8) { w.buf = append(w.buf, b) }

func (w *writer) u16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }

func (w *writer) u32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }

// cOctetString appends s and its NUL terminator.
func (w *writer) cOctetString(s string) {
	w.buf = append(w.buf, s...)
	w.buf = append(w.buf, 0)
}

// octets appends raw bytes with no length prefix or terminator.
func (w *writer) octets(b []byte) { w.buf = append(w.buf, b...) }
