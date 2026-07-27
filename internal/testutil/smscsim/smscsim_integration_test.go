package smscsim_test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/testutil/smscsim"
)

const (
	statusThrottled uint32 = 0x00000058 // ESME_RTHROTTLED
	cmdSubmitSMResp uint32 = 0x80000004
)

// readRespHeader reads one SMPP PDU's 16-byte header (command_id, command_status), draining its body.
// It bypasses the full-PDU codec so an error response with an empty body (e.g. a throttled submit_sm_
// resp with no message_id — valid SMPP) is read for its status without a body-parse failure.
func readRespHeader(t *testing.T, conn net.Conn) (cmdID, status uint32) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	hdr := make([]byte, 16)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatalf("read pdu header: %v", err)
	}
	length := binary.BigEndian.Uint32(hdr[0:4])
	cmdID = binary.BigEndian.Uint32(hdr[4:8])
	status = binary.BigEndian.Uint32(hdr[8:12])
	if length > 16 {
		if _, err := io.CopyN(io.Discard, conn, int64(length-16)); err != nil {
			t.Fatalf("drain pdu body: %v", err)
		}
	}
	return cmdID, status
}

// bindTx opens a transmitter bind to the simulator, failing the test on a rejected bind.
func bindTx(t *testing.T, addr, systemID, password string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := smpp.WritePDU(conn, smpp.PDU{Sequence: 1, Body: &smpp.BindTransmitter{
		BindFields: smpp.BindFields{SystemID: systemID, Password: password, InterfaceVersion: 0x34},
	}}); err != nil {
		t.Fatalf("write bind: %v", err)
	}
	if _, status := readRespHeader(t, conn); status != smpp.StatusOK {
		t.Fatalf("bind rejected: command_status 0x%08x", status)
	}
	return conn
}

// submit sends one submit_sm and returns its command_status, skipping any interleaved non-submit PDUs.
func submit(t *testing.T, conn net.Conn, seq uint32, dest string) uint32 {
	t.Helper()
	if err := smpp.WritePDU(conn, smpp.PDU{Sequence: seq, Body: &smpp.SubmitSM{SMFields: smpp.SMFields{
		SourceAddrTON: 1, SourceAddrNPI: 1, SourceAddr: "33100",
		DestAddrTON: 1, DestAddrNPI: 1, DestinationAddr: dest,
		ShortMessage: []byte("smoke"),
	}}}); err != nil {
		t.Fatalf("write submit: %v", err)
	}
	for i := 0; i < 5; i++ {
		if cmdID, status := readRespHeader(t, conn); cmdID == cmdSubmitSMResp {
			return status
		}
		// Skip an interleaved enquire_link / deliver_sm and read again.
	}
	t.Fatal("no submit_sm_resp after 5 PDUs")
	return 0
}

// TestSimHealthyBindAndSubmit: the real simulator starts on the healthy profile, accepts a bind,
// answers a submit_sm with ESME_ROK, and serves its read-only control API.
func TestSimHealthyBindAndSubmit(t *testing.T) {
	sim := smscsim.Launch(t, smscsim.HealthyConfig("smppclient1", "secret"))

	if err := sim.Health(context.Background()); err != nil {
		t.Fatalf("control health: %v", err)
	}
	inventory, err := sim.VirtualSMSCs(context.Background())
	if err != nil {
		t.Fatalf("virtual-smscs: %v", err)
	}
	if len(inventory) == 0 {
		t.Error("virtual-smscs inventory is empty, want the configured SMSC")
	}

	conn := bindTx(t, sim.SMPPAddr, "smppclient1", "secret")
	if status := submit(t, conn, 2, "33600000001"); status != smpp.StatusOK {
		t.Errorf("healthy submit command_status = 0x%08x, want ESME_ROK", status)
	}
}

// TestSimThrottlingScenario: the throttling-carrier profile returns ESME_RTHROTTLED beyond its rate cap
// — proving a fault scenario is driveable from the harness (via config, the simulator's only input).
func TestSimThrottlingScenario(t *testing.T) {
	sim := smscsim.Launch(t, smscsim.ThrottlingConfig("smppclient1", "secret", 1))
	conn := bindTx(t, sim.SMPPAddr, "smppclient1", "secret")

	// With a 1/s cap, a burst of submits within one second must draw at least one RTHROTTLED.
	throttled := false
	for i := 0; i < 10; i++ {
		if submit(t, conn, uint32(2+i), "33600000001") == statusThrottled {
			throttled = true
			break
		}
	}
	if !throttled {
		t.Error("throttling-carrier never returned ESME_RTHROTTLED over a 10-message burst at cap 1/s")
	}
}
