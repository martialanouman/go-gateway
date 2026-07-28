package connectorpool

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// tightRejectSMSC starts a minimal SMSC that rejects the bind and closes the socket the way a real SMSC
// does — reproducing BOTH faults this test guards: (1) it sends a BODYLESS error bind_transceiver_resp (a
// bare 16-octet header, no system_id — SMPP v3.4 §4.1.2 omits the body of an error response), which the
// codec must decode rather than reject as ErrMalformedBody; and (2) it closes IMMEDIATELY on the same
// goroutine, racing the response dispatch on the client. fakesmsc masks both — it writes an (empty-body,
// 17-octet) reject and sequences its close far apart — so the raw framing here is deliberate.
func tightRejectSMSC(t *testing.T, status uint32) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				pdu, err := smpp.ReadPDU(c)
				if err != nil {
					_ = c.Close()
					return
				}
				// A bodyless error bind_transceiver_resp: command_length=16, id=0x80000009, the error
				// command_status, the echoed sequence — no body, exactly like a real SMSC rejection.
				hdr := make([]byte, 16)
				binary.BigEndian.PutUint32(hdr[0:], 16)
				binary.BigEndian.PutUint32(hdr[4:], 0x80000009)
				binary.BigEndian.PutUint32(hdr[8:], status)
				binary.BigEndian.PutUint32(hdr[12:], pdu.Sequence)
				_, _ = c.Write(hdr)
				_ = c.Close() // immediate close — races the response dispatch on the client
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// TestDialAndBindRejectIsPermanentNotLinkDrop guards the reject-then-close fix: a well-behaved SMSC that
// rejects the bind with a non-OK command_status AND closes the socket must surface as a permanent
// BindRejectedError (which the reconnect loop treats as fatal and STOPS on), never as a racy errBindClosed
// (a link drop it would retry forever against credentials that can never work). The response and the
// close both signal roundtrip's (unordered) select, so without the fix — preferring a dispatched response
// over the close signal — the classification is a coin flip. The loop + -race exercise that window.
func TestDialAndBindRejectIsPermanentNotLinkDrop(t *testing.T) {
	addr := tightRejectSMSC(t, errs.StatusInvalidPasswd)
	cfg := BindConfig{
		Addr: addr, SystemID: "esme", Password: "pw",
		DialTimeout: 3 * time.Second, ResponseTimeout: 3 * time.Second,
		EnquireLinkInterval: time.Minute, EnquireLinkMaxMissed: 3, WindowSize: 10,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	onDeliver := func(context.Context, *smpp.DeliverSM) error { return nil }

	for i := 0; i < 100; i++ {
		b, err := dialAndBind(context.Background(), cfg, logger, onDeliver)
		if b != nil {
			b.Close()
			t.Fatalf("iter %d: dialAndBind returned a bind, want a rejection", i)
		}
		var rej *BindRejectedError
		if !errors.As(err, &rej) {
			t.Fatalf("iter %d: err = %v (%T), want a BindRejectedError — a reject-then-close must be permanent, not a link drop", i, err, err)
		}
		if rej.Status != errs.StatusInvalidPasswd {
			t.Fatalf("iter %d: reject status = 0x%08x, want ESME_RINVPASWD", i, rej.Status)
		}
	}
}
