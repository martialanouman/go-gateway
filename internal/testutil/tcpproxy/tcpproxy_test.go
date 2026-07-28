package tcpproxy_test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/testutil/tcpproxy"
)

// echoServer starts a line-echo TCP server on loopback and returns its address.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				s := bufio.NewScanner(c)
				for s.Scan() {
					_, _ = c.Write(append(s.Bytes(), '\n'))
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

// TestProxyForwards: a message sent through the proxy is echoed back by the upstream.
func TestProxyForwards(t *testing.T) {
	p := tcpproxy.New(t, echoServer(t))
	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "ping\n" {
		t.Errorf("echo = %q, want %q", got, "ping\n")
	}
}

// TestProxyCutAndResume: Cut drops the live connection and refuses new dials; Resume restores forwarding.
func TestProxyCutAndResume(t *testing.T) {
	p := tcpproxy.New(t, echoServer(t))

	live, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = live.Close() }()

	p.Cut()

	// The live connection is dropped: a read returns an error (EOF/reset) rather than hanging.
	_ = live.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := live.Read(make([]byte, 1)); err == nil {
		t.Error("read on a cut connection succeeded, want an error")
	}

	// A dial while cut is accepted then immediately closed: the first read fails.
	refused, err := net.Dial("tcp", p.Addr())
	if err == nil {
		_ = refused.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, rerr := refused.Read(make([]byte, 1)); rerr == nil {
			t.Error("a connection dialled while cut stayed open, want it closed")
		}
		_ = refused.Close()
	}

	p.Resume()

	after, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatalf("dial after resume: %v", err)
	}
	defer func() { _ = after.Close() }()
	if _, err := after.Write([]byte("back\n")); err != nil {
		t.Fatalf("write after resume: %v", err)
	}
	_ = after.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := bufio.NewReader(after).ReadString('\n')
	if err != nil {
		t.Fatalf("read after resume: %v", err)
	}
	if got != "back\n" {
		t.Errorf("echo after resume = %q, want %q", got, "back\n")
	}
}
