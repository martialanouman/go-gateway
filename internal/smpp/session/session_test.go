package session_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/smpp/session"
)

// discardLogger silences the session's structured logs (a recovered panic logs an Error) so test
// output stays clean.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

const ioDeadline = 3 * time.Second

// newSession wires a Session onto one end of an in-memory net.Pipe and runs it. The returned client
// conn speaks raw PDUs to the session; errc yields Serve's result once. cancel and the client are
// torn down on cleanup.
func newSession(t *testing.T, cfg session.Config) (client net.Conn, sess *session.Session, cancel context.CancelFunc, errc <-chan error) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	sess = session.New(serverConn, cfg)
	ctx, cancelFn := context.WithCancel(context.Background())
	ec := make(chan error, 1)
	go func() { ec <- sess.Serve(ctx) }()
	t.Cleanup(func() {
		cancelFn()
		_ = clientConn.Close()
	})
	return clientConn, sess, cancelFn, ec
}

func writePDU(t *testing.T, c net.Conn, pdu smpp.PDU) {
	t.Helper()
	_ = c.SetWriteDeadline(time.Now().Add(ioDeadline))
	if err := smpp.WritePDU(c, pdu); err != nil {
		t.Fatalf("write %#x: %v", pdu.CommandID(), err)
	}
}

func readPDU(t *testing.T, c net.Conn) smpp.PDU {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(ioDeadline))
	pdu, err := smpp.ReadPDU(c)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return pdu
}

// roundtrip writes a request and reads the session's response.
func roundtrip(t *testing.T, c net.Conn, pdu smpp.PDU) smpp.PDU {
	t.Helper()
	writePDU(t, c, pdu)
	return readPDU(t, c)
}

func waitErr(t *testing.T, errc <-chan error) error {
	t.Helper()
	select {
	case err := <-errc:
		return err
	case <-time.After(ioDeadline):
		t.Fatal("Serve did not return in time")
		return nil
	}
}

func bindReq(mode session.BindMode, seq uint32) smpp.PDU {
	f := smpp.BindFields{SystemID: "esme", Password: "secret", InterfaceVersion: smpp.InterfaceVersion34}
	var body smpp.Body
	switch mode {
	case session.BindReceiver:
		body = &smpp.BindReceiver{BindFields: f}
	case session.BindTransceiver:
		body = &smpp.BindTransceiver{BindFields: f}
	default:
		body = &smpp.BindTransmitter{BindFields: f}
	}
	return smpp.PDU{Sequence: seq, Body: body}
}

func wantBindRespCmd(mode session.BindMode) smpp.CommandID {
	switch mode {
	case session.BindReceiver:
		return smpp.CmdBindReceiverResp
	case session.BindTransceiver:
		return smpp.CmdBindTransceiverResp
	default:
		return smpp.CmdBindTransmitterResp
	}
}

func bindRespFields(t *testing.T, body smpp.Body) smpp.BindRespFields {
	t.Helper()
	switch b := body.(type) {
	case *smpp.BindTransmitterResp:
		return b.BindRespFields
	case *smpp.BindReceiverResp:
		return b.BindRespFields
	case *smpp.BindTransceiverResp:
		return b.BindRespFields
	default:
		t.Fatalf("not a bind response: %T", body)
		return smpp.BindRespFields{}
	}
}

// bindOK establishes a bound session in the given mode, asserting the handshake succeeds.
func bindOK(t *testing.T, c net.Conn, mode session.BindMode) {
	t.Helper()
	resp := roundtrip(t, c, bindReq(mode, 1))
	if resp.Status != smpp.StatusOK {
		t.Fatalf("bind %s: status = %#x, want StatusOK", mode, resp.Status)
	}
}

func TestSession_BindEnquireUnbind(t *testing.T) {
	t.Parallel()
	const systemID = "gw-test"
	for _, mode := range []session.BindMode{session.BindTransmitter, session.BindReceiver, session.BindTransceiver} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			client, _, _, errc := newSession(t, session.Config{SystemID: systemID})

			resp := roundtrip(t, client, bindReq(mode, 1))
			if resp.CommandID() != wantBindRespCmd(mode) {
				t.Fatalf("bind resp cmd = %#x, want %#x", resp.CommandID(), wantBindRespCmd(mode))
			}
			if resp.Status != smpp.StatusOK {
				t.Fatalf("bind status = %#x, want StatusOK", resp.Status)
			}
			fields := bindRespFields(t, resp.Body)
			if fields.SystemID != systemID {
				t.Errorf("bind resp system_id = %q, want %q", fields.SystemID, systemID)
			}
			if v, ok := fields.TLVs.Get(smpp.TagSCInterfaceVersion); !ok || !bytes.Equal(v, []byte{smpp.InterfaceVersion34}) {
				t.Errorf("sc_interface_version TLV = %v (ok=%v), want [0x34]", v, ok)
			}

			if resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: &smpp.EnquireLink{}}); resp.CommandID() != smpp.CmdEnquireLinkResp {
				t.Errorf("enquire_link resp cmd = %#x, want enquire_link_resp", resp.CommandID())
			}

			if resp := roundtrip(t, client, smpp.PDU{Sequence: 3, Body: &smpp.Unbind{}}); resp.CommandID() != smpp.CmdUnbindResp {
				t.Errorf("unbind resp cmd = %#x, want unbind_resp", resp.CommandID())
			}
			if err := waitErr(t, errc); err != nil {
				t.Errorf("Serve after unbind = %v, want nil", err)
			}
		})
	}
}

func TestSession_SubmitBeforeBind(t *testing.T) {
	t.Parallel()
	client, _, _, _ := newSession(t, session.Config{})

	resp := roundtrip(t, client, smpp.PDU{Sequence: 1, Body: &smpp.SubmitSM{}})
	if resp.CommandID() != smpp.CmdSubmitSMResp {
		t.Fatalf("resp cmd = %#x, want submit_sm_resp", resp.CommandID())
	}
	if resp.Status != errs.StatusInvalidBindStatus {
		t.Errorf("status = %#x, want ESME_RINVBNDSTS (%#x)", resp.Status, errs.StatusInvalidBindStatus)
	}
	// The session survives an out-of-sequence submit.
	if resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: &smpp.EnquireLink{}}); resp.CommandID() != smpp.CmdEnquireLinkResp {
		t.Errorf("session did not survive: enquire resp cmd = %#x", resp.CommandID())
	}
}

func TestSession_ReceiverCannotSubmit(t *testing.T) {
	t.Parallel()
	client, _, _, _ := newSession(t, session.Config{})
	bindOK(t, client, session.BindReceiver)

	resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{}})
	if resp.Status != errs.StatusInvalidBindStatus {
		t.Errorf("receiver submit status = %#x, want ESME_RINVBNDSTS", resp.Status)
	}
}

func TestSession_SubmitAfterBind(t *testing.T) {
	t.Parallel()
	got := make(chan session.SubmitRequest, 1)
	cfg := session.Config{
		OnSubmit: func(_ context.Context, req session.SubmitRequest) session.SubmitResult {
			got <- req
			return session.SubmitResult{Status: smpp.StatusOK, MessageID: "id-42"}
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	sm := &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddr:      "1234",
		DestinationAddr: "22990001111",
		ShortMessage:    []byte("hello"),
	}}
	resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: sm})
	if resp.Status != smpp.StatusOK {
		t.Fatalf("submit status = %#x, want StatusOK", resp.Status)
	}
	out, ok := resp.Body.(*smpp.SubmitSMResp)
	if !ok {
		t.Fatalf("resp body = %T, want *smpp.SubmitSMResp", resp.Body)
	}
	if out.MessageID != "id-42" {
		t.Errorf("message id = %q, want %q", out.MessageID, "id-42")
	}

	select {
	case req := <-got:
		if req.Source != "1234" || req.Destination != "22990001111" {
			t.Errorf("hook req = {src:%q dst:%q}, want {1234 22990001111}", req.Source, req.Destination)
		}
		if string(req.Body.Reveal()) != "hello" {
			t.Errorf("hook body = %q, want %q", req.Body.Reveal(), "hello")
		}
	default:
		t.Fatal("OnSubmit was not called")
	}
}

func unknownFrame(seq uint32) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint32(b[0:4], 16)         // command_length
	binary.BigEndian.PutUint32(b[4:8], 0x00000103) // unsupported command id
	binary.BigEndian.PutUint32(b[8:12], 0)         // command_status
	binary.BigEndian.PutUint32(b[12:16], seq)      // sequence_number
	return b
}

func TestSession_UnknownCommand(t *testing.T) {
	t.Parallel()
	client, _, _, _ := newSession(t, session.Config{})

	_ = client.SetWriteDeadline(time.Now().Add(ioDeadline))
	if _, err := client.Write(unknownFrame(7)); err != nil {
		t.Fatalf("write raw frame: %v", err)
	}
	resp := readPDU(t, client)
	if resp.CommandID() != smpp.CmdGenericNACK {
		t.Fatalf("resp cmd = %#x, want generic_nack", resp.CommandID())
	}
	if resp.Status != errs.StatusInvalidCmdID {
		t.Errorf("status = %#x, want ESME_RINVCMDID (%#x)", resp.Status, errs.StatusInvalidCmdID)
	}
	// The sequence of an undecodable frame is unrecoverable, so the nack carries 0.
	if resp.Sequence != 0 {
		t.Errorf("nack sequence = %d, want 0", resp.Sequence)
	}
	// Framing stays intact: the session keeps serving.
	if resp := roundtrip(t, client, smpp.PDU{Sequence: 8, Body: &smpp.EnquireLink{}}); resp.CommandID() != smpp.CmdEnquireLinkResp {
		t.Errorf("session did not survive: enquire resp cmd = %#x", resp.CommandID())
	}
}

func TestSession_ReBind(t *testing.T) {
	t.Parallel()
	client, _, _, _ := newSession(t, session.Config{})
	bindOK(t, client, session.BindTransceiver)

	resp := roundtrip(t, client, bindReq(session.BindTransceiver, 2))
	if resp.CommandID() != smpp.CmdBindTransceiverResp {
		t.Fatalf("re-bind resp cmd = %#x, want bind_transceiver_resp", resp.CommandID())
	}
	if resp.Status != errs.StatusAlreadyBound {
		t.Errorf("re-bind status = %#x, want ESME_RALYBND (%#x)", resp.Status, errs.StatusAlreadyBound)
	}
}

func TestSession_WindowSaturation(t *testing.T) {
	t.Parallel()
	client, sess, _, _ := newSession(t, session.Config{WindowSize: 1, ResponseTimeout: ioDeadline})
	bindOK(t, client, session.BindTransceiver)

	deliver := func() smpp.Body {
		return &smpp.DeliverSM{SMFields: smpp.SMFields{SourceAddr: "x", DestinationAddr: "y", ShortMessage: []byte("mo")}}
	}
	type sendResult struct {
		pdu smpp.PDU
		err error
	}

	// First Send acquires the only window slot and pushes deliver_sm #1.
	res1 := make(chan sendResult, 1)
	go func() {
		pdu, err := sess.Send(context.Background(), deliver())
		res1 <- sendResult{pdu, err}
	}()
	pdu1 := readPDU(t, client) // consume #1, but do not answer yet
	if pdu1.CommandID() != smpp.CmdDeliverSM {
		t.Fatalf("first push cmd = %#x, want deliver_sm", pdu1.CommandID())
	}

	// Second Send must block on the saturated window: nothing reaches the client.
	res2 := make(chan sendResult, 1)
	go func() {
		pdu, err := sess.Send(context.Background(), deliver())
		res2 <- sendResult{pdu, err}
	}()
	_ = client.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := smpp.ReadPDU(client); err == nil {
		t.Fatal("second push reached client while window was saturated")
	} else if nerr := net.Error(nil); !errors.As(err, &nerr) || !nerr.Timeout() {
		t.Fatalf("second read error = %v, want timeout", err)
	}

	// Answer #1: its slot frees, unblocking #2.
	writePDU(t, client, smpp.PDU{Sequence: pdu1.Sequence, Body: &smpp.DeliverSMResp{}})
	if r := <-res1; r.err != nil {
		t.Fatalf("Send #1 = %v, want nil", r.err)
	}

	pdu2 := readPDU(t, client) // #2 now flows
	if pdu2.CommandID() != smpp.CmdDeliverSM {
		t.Fatalf("second push cmd = %#x, want deliver_sm", pdu2.CommandID())
	}
	writePDU(t, client, smpp.PDU{Sequence: pdu2.Sequence, Body: &smpp.DeliverSMResp{}})
	if r := <-res2; r.err != nil {
		t.Fatalf("Send #2 = %v, want nil", r.err)
	}
}

func TestSession_StateTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		setup      session.BindMode // bind to run first; ignored unless hasSetup
		hasSetup   bool
		input      smpp.PDU
		wantCmd    smpp.CommandID
		wantStatus uint32
	}{
		{
			name:       "open/submit rejected",
			input:      smpp.PDU{Sequence: 10, Body: &smpp.SubmitSM{}},
			wantCmd:    smpp.CmdSubmitSMResp,
			wantStatus: errs.StatusInvalidBindStatus,
		},
		{
			name:       "open/enquire ok",
			input:      smpp.PDU{Sequence: 10, Body: &smpp.EnquireLink{}},
			wantCmd:    smpp.CmdEnquireLinkResp,
			wantStatus: smpp.StatusOK,
		},
		{
			name:       "tx/submit ok",
			setup:      session.BindTransmitter,
			hasSetup:   true,
			input:      smpp.PDU{Sequence: 10, Body: &smpp.SubmitSM{}},
			wantCmd:    smpp.CmdSubmitSMResp,
			wantStatus: smpp.StatusOK,
		},
		{
			name:       "rx/submit rejected",
			setup:      session.BindReceiver,
			hasSetup:   true,
			input:      smpp.PDU{Sequence: 10, Body: &smpp.SubmitSM{}},
			wantCmd:    smpp.CmdSubmitSMResp,
			wantStatus: errs.StatusInvalidBindStatus,
		},
		{
			name:       "trx/submit ok",
			setup:      session.BindTransceiver,
			hasSetup:   true,
			input:      smpp.PDU{Sequence: 10, Body: &smpp.SubmitSM{}},
			wantCmd:    smpp.CmdSubmitSMResp,
			wantStatus: smpp.StatusOK,
		},
		{
			name:       "trx/re-bind rejected",
			setup:      session.BindTransceiver,
			hasSetup:   true,
			input:      bindReq(session.BindTransceiver, 10),
			wantCmd:    smpp.CmdBindTransceiverResp,
			wantStatus: errs.StatusAlreadyBound,
		},
		{
			name:       "tx/enquire ok",
			setup:      session.BindTransmitter,
			hasSetup:   true,
			input:      smpp.PDU{Sequence: 10, Body: &smpp.EnquireLink{}},
			wantCmd:    smpp.CmdEnquireLinkResp,
			wantStatus: smpp.StatusOK,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, _, _, _ := newSession(t, session.Config{})
			if tc.hasSetup {
				bindOK(t, client, tc.setup)
			}
			resp := roundtrip(t, client, tc.input)
			if resp.CommandID() != tc.wantCmd {
				t.Errorf("resp cmd = %#x, want %#x", resp.CommandID(), tc.wantCmd)
			}
			if resp.Status != tc.wantStatus {
				t.Errorf("resp status = %#x, want %#x", resp.Status, tc.wantStatus)
			}
		})
	}
}

// syncBuffer is a race-safe io.Writer for capturing log output written on the session goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestSession_DoesNotLogBody pins invariant (a): the short_message never reaches a log, only its
// redacted placeholder does.
func TestSession_DoesNotLogBody(t *testing.T) {
	t.Parallel()
	const canary = "SHORTMSG_CANARY_9f3_secret_code_123456"
	var buf syncBuffer
	cfg := session.Config{
		Logger: slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	sm := &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddr:      "1234",
		DestinationAddr: "5678",
		ShortMessage:    []byte(canary),
	}}
	// Reading the response happens-after the session logs the submit, so the buffer is complete.
	_ = roundtrip(t, client, smpp.PDU{Sequence: 2, Body: sm})

	out := buf.String()
	if strings.Contains(out, canary) {
		t.Fatalf("INVARIANT (a) VIOLATED: log contains the message body:\n%s", out)
	}
	if !strings.Contains(out, msg.Redacted) {
		t.Errorf("expected %q in log output, got:\n%s", msg.Redacted, out)
	}
}

// TestSession_BindHookPanicRecovered pins that a panicking OnBind rejects the bind with
// ESME_RSYSERR instead of tearing down the read goroutine, and the session keeps serving.
func TestSession_BindHookPanicRecovered(t *testing.T) {
	t.Parallel()
	cfg := session.Config{
		Logger: discardLogger(),
		OnBind: func(context.Context, session.BindRequest) session.BindResult {
			panic("boom in OnBind")
		},
	}
	client, _, _, _ := newSession(t, cfg)

	resp := roundtrip(t, client, bindReq(session.BindTransmitter, 1))
	if resp.CommandID() != smpp.CmdBindTransmitterResp {
		t.Fatalf("resp cmd = %#x, want bind_transmitter_resp", resp.CommandID())
	}
	if resp.Status != errs.StatusSysErr {
		t.Errorf("bind status = %#x, want ESME_RSYSERR (%#x)", resp.Status, errs.StatusSysErr)
	}
	// The session survived the panic: it stays open (bind was rejected) and still answers.
	if r := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: &smpp.EnquireLink{}}); r.CommandID() != smpp.CmdEnquireLinkResp {
		t.Errorf("session did not survive bind panic: enquire resp cmd = %#x", r.CommandID())
	}
}

// TestSession_SubmitHookPanicRecovered pins that a panicking OnSubmit rejects the submit_sm with
// ESME_RSYSERR and the session keeps serving.
func TestSession_SubmitHookPanicRecovered(t *testing.T) {
	t.Parallel()
	cfg := session.Config{
		Logger: discardLogger(),
		OnSubmit: func(context.Context, session.SubmitRequest) session.SubmitResult {
			panic("boom in OnSubmit")
		},
	}
	client, _, _, _ := newSession(t, cfg)
	bindOK(t, client, session.BindTransmitter)

	resp := roundtrip(t, client, smpp.PDU{Sequence: 2, Body: &smpp.SubmitSM{}})
	if resp.CommandID() != smpp.CmdSubmitSMResp {
		t.Fatalf("resp cmd = %#x, want submit_sm_resp", resp.CommandID())
	}
	if resp.Status != errs.StatusSysErr {
		t.Errorf("submit status = %#x, want ESME_RSYSERR (%#x)", resp.Status, errs.StatusSysErr)
	}
	if r := roundtrip(t, client, smpp.PDU{Sequence: 3, Body: &smpp.EnquireLink{}}); r.CommandID() != smpp.CmdEnquireLinkResp {
		t.Errorf("session did not survive submit panic: enquire resp cmd = %#x", r.CommandID())
	}
}

// TestSession_IdleTimeout pins that a peer gone silent past Config.IdleTimeout is dropped as an
// orderly close (Serve returns nil) and its connection is closed.
func TestSession_IdleTimeout(t *testing.T) {
	t.Parallel()
	client, _, _, errc := newSession(t, session.Config{
		Logger:      discardLogger(),
		IdleTimeout: 150 * time.Millisecond,
	})
	bindOK(t, client, session.BindTransceiver)

	// Send nothing further: the read deadline must fire and end the session on its own.
	if err := waitErr(t, errc); err != nil {
		t.Errorf("Serve after idle timeout = %v, want nil", err)
	}
	// The session closed its side: a client read observes it.
	_ = client.SetReadDeadline(time.Now().Add(ioDeadline))
	if _, err := smpp.ReadPDU(client); err == nil {
		t.Error("expected read error after idle timeout closed the connection")
	}
}

// TestSession_IdleTimeoutResetsOnActivity pins that the idle deadline measures inactivity, not
// total session age: a peer that keeps sending within IdleTimeout is never dropped. An absolute
// (set-once) deadline would fail this; the per-read reset makes it pass.
func TestSession_IdleTimeoutResetsOnActivity(t *testing.T) {
	t.Parallel()
	const idle = 300 * time.Millisecond
	client, _, _, errc := newSession(t, session.Config{Logger: discardLogger(), IdleTimeout: idle})
	bindOK(t, client, session.BindTransceiver)

	// Stay active with a gap well under IdleTimeout across several rounds (~3x margin for CI/race).
	for i := 0; i < 5; i++ {
		time.Sleep(idle / 3)
		r := roundtrip(t, client, smpp.PDU{Sequence: uint32(100 + i), Body: &smpp.EnquireLink{}})
		if r.CommandID() != smpp.CmdEnquireLinkResp {
			t.Fatalf("round %d: session dropped under activity, resp cmd = %#x", i, r.CommandID())
		}
	}
	// Serve is still running: the total elapsed time exceeds IdleTimeout, yet no close occurred.
	select {
	case err := <-errc:
		t.Fatalf("Serve returned early = %v, want still running", err)
	default:
	}
}

func TestSession_CleanCloseOnEOF(t *testing.T) {
	t.Parallel()
	client, _, _, errc := newSession(t, session.Config{})
	bindOK(t, client, session.BindTransceiver)

	if err := client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	if err := waitErr(t, errc); err != nil {
		t.Errorf("Serve after peer close = %v, want nil", err)
	}
}

func TestSession_ContextCancel(t *testing.T) {
	t.Parallel()
	client, _, cancel, errc := newSession(t, session.Config{})
	bindOK(t, client, session.BindTransceiver)

	cancel()
	if err := waitErr(t, errc); err != nil {
		t.Errorf("Serve after ctx cancel = %v, want nil", err)
	}
	// The socket is closed: a further read from the client observes it.
	_ = client.SetReadDeadline(time.Now().Add(ioDeadline))
	if _, err := smpp.ReadPDU(client); err == nil {
		t.Error("expected read error after ctx cancel closed the connection")
	}
}
