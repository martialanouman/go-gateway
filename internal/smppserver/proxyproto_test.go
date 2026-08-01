package smppserver

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

// dialAndRead opens a TCP connection to lis, writes raw (a PROXY header, or nothing), then accepts the
// server side and returns the remote IP the server observes — which is exactly what remoteIP feeds to the
// bind throttle.
func dialAndRead(t *testing.T, lis net.Listener, raw string) string {
	t.Helper()

	type result struct {
		ip  string
		err error
	}
	done := make(chan result, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = conn.Close() }()
		// The proxyproto listener parses the header lazily, on the first read — so a test that only
		// called RemoteAddr() would pass even with a broken decode. Force the read first.
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
		done <- result{ip: remoteIP(conn)}
	}()

	c, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	// Header and payload go out in ONE write. Split across two, the server can read its byte and close
	// while the second write is still in flight, and the peer answers RST — a flake that depends purely on
	// scheduling (it passed locally and failed on CI). The payload byte is what makes the server's Read
	// return once the header, if any, has been consumed.
	//
	// A write error is not fatal here: the server legitimately closes as soon as it has what it needs, so
	// the assertion belongs on the address it observed. A write that truly failed shows up as the timeout
	// below, with a clearer message.
	if _, err := c.Write([]byte(raw + "x")); err != nil {
		t.Logf("write (server may have already closed): %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("accept: %v", r.err)
		}
		return r.ip
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server to observe the connection")
		return ""
	}
}

func tcpListener(t *testing.T) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	return lis
}

// TestProxyProtocolReportsTheRealClientIP is the fix for step-026's operational gap: behind an L4 balancer
// every bind arrives from the balancer's address, so the per-IP anti-brute-force counter collapses onto a
// single bindfail:ip key and one abusive client locks out everyone (a self-DoS). With the PROXY header
// trusted from the balancer's range, the throttle sees the ESME's real address again.
func TestProxyProtocolReportsTheRealClientIP(t *testing.T) {
	wrapped, err := wrapProxyProtocol(tcpListener(t), []string{"127.0.0.0/8"})
	if err != nil {
		t.Fatalf("wrapProxyProtocol: %v", err)
	}

	got := dialAndRead(t, wrapped, "PROXY TCP4 203.0.113.7 10.0.0.1 56324 2775\r\n")
	if got != "203.0.113.7" {
		t.Errorf("remoteIP = %q, want the announced client 203.0.113.7 — the throttle would key on the balancer", got)
	}
}

// TestProxyProtocolKeepsClientsDistinct proves the property that actually matters: two ESMEs arriving
// through the SAME balancer are told apart. Without it their bind failures share one counter and either
// can lock out the other.
func TestProxyProtocolKeepsClientsDistinct(t *testing.T) {
	wrapped, err := wrapProxyProtocol(tcpListener(t), []string{"127.0.0.0/8"})
	if err != nil {
		t.Fatalf("wrapProxyProtocol: %v", err)
	}

	first := dialAndRead(t, wrapped, "PROXY TCP4 203.0.113.7 10.0.0.1 56324 2775\r\n")
	second := dialAndRead(t, wrapped, "PROXY TCP4 198.51.100.4 10.0.0.1 41010 2775\r\n")
	if first == second {
		t.Fatalf("both connections resolved to %q — the two ESMEs share one throttle key", first)
	}
}

// TestProxyProtocolDropsUntrustedPeer is the security guard. REQUIRE alone is NOT enough: it only mandates
// that a header be present, and still believes one sent by ANY peer — so a client reaching the port
// directly could forge a source address to escape its own throttle or poison another client's counter.
// Restricting to trusted ranges makes the listener DROP such a connection outright rather than merely
// ignore its header: serving it raw would share one port between PROXY and non-PROXY traffic, which the
// protocol's security model forbids.
//
// OPERATIONAL CONSEQUENCE: enabling trusted ranges closes the port to everything that is not the balancer.
// That is the intended posture behind an L4 LB, and it is why the feature stays off by default.
func TestProxyProtocolDropsUntrustedPeer(t *testing.T) {
	// The dialer is loopback, so trusting only a documentation range makes this peer untrusted.
	wrapped, err := wrapProxyProtocol(tcpListener(t), []string{"192.0.2.0/24"})
	if err != nil {
		t.Fatalf("wrapProxyProtocol: %v", err)
	}

	accepted := make(chan struct{}, 1)
	go func() {
		for {
			conn, err := wrapped.Accept()
			if err != nil {
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, err := conn.Read(make([]byte, 1)); err == nil {
				accepted <- struct{}{}
			}
			_ = conn.Close()
		}
	}()

	c, err := net.Dial("tcp", wrapped.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	_, _ = c.Write([]byte("PROXY TCP4 203.0.113.7 10.0.0.1 56324 2775\r\nx"))

	select {
	case <-accepted:
		t.Fatal("a forged PROXY header from an untrusted peer reached the SMPP session — a client could spoof its throttle key")
	case <-time.After(500 * time.Millisecond):
		// The connection was dropped before ever becoming a session. Expected.
	}
}

// TestProxyProtocolSurvivesAnUntrustedPeer proves a rejected connection does not stop the accept loop. If
// it did, any host able to reach the port could take the whole SMPP listener down with a single dial —
// trading a throttle self-DoS for a far worse one.
func TestProxyProtocolSurvivesAnUntrustedPeer(t *testing.T) {
	// Trust loopback so a legitimate connection works, then prove a bogus upstream cannot wedge the loop.
	wrapped, err := wrapProxyProtocol(tcpListener(t), []string{"127.0.0.0/8"})
	if err != nil {
		t.Fatalf("wrapProxyProtocol: %v", err)
	}

	// A persistent accept loop, as the real listener runs: only connections whose header parsed feed the
	// channel, so a dropped or malformed one is simply skipped rather than consumed by the next assertion.
	served := make(chan string, 2)
	go func() {
		for {
			conn, err := wrapped.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				if _, err := conn.Read(make([]byte, 1)); err == nil {
					served <- remoteIP(conn)
				}
			}()
		}
	}()

	// A malformed header must not wedge the loop.
	bad, err := net.Dial("tcp", wrapped.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_, _ = bad.Write([]byte("PROXY GARBAGE\r\nx"))
	_ = bad.Close()

	// The listener must still serve the next, legitimate client.
	good, err := net.Dial("tcp", wrapped.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = good.Close() }()
	_, _ = good.Write([]byte("PROXY TCP4 203.0.113.7 10.0.0.1 56324 2775\r\nx"))

	select {
	case got := <-served:
		if got != "203.0.113.7" {
			t.Errorf("remoteIP = %q, want 203.0.113.7", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no client served after a malformed connection — the accept loop did not recover")
	}
}

// TestProxyProtocolDisabledByDefault proves the no-config path is untouched: with no trusted ranges the
// listener is not wrapped at all, so a deployment without a balancer keeps the transport peer address and
// cannot be tricked by a header.
func TestProxyProtocolDisabledByDefault(t *testing.T) {
	lis := tcpListener(t)
	wrapped, err := wrapProxyProtocol(lis, nil)
	if err != nil {
		t.Fatalf("wrapProxyProtocol: %v", err)
	}
	if wrapped != lis {
		t.Fatal("listener was wrapped although no trusted range is configured")
	}

	got := dialAndRead(t, wrapped, "PROXY TCP4 203.0.113.7 10.0.0.1 56324 2775\r\n")
	if got != "127.0.0.1" {
		t.Errorf("remoteIP = %q, want the real peer 127.0.0.1 (header must not be honoured when disabled)", got)
	}
}

// TestProxyProtocolRejectsBadCIDR proves a malformed range fails at startup rather than silently
// disabling the protection — a typo in a deployment manifest must not degrade to "trust nobody, throttle
// everybody together".
func TestProxyProtocolRejectsBadCIDR(t *testing.T) {
	for _, bad := range []string{"not-a-cidr", "10.0.0.1", ""} {
		t.Run(fmt.Sprintf("%q", bad), func(t *testing.T) {
			if _, err := wrapProxyProtocol(tcpListener(t), []string{bad}); err == nil {
				t.Errorf("wrapProxyProtocol(%q) = nil error, want a startup failure", bad)
			}
		})
	}
}
