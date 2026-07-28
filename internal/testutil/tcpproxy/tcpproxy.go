// Package tcpproxy is an in-process TCP relay for resilience tests. A client dials Proxy.Addr() and the
// proxy forwards the stream to a fixed upstream; Cut() closes every live connection and refuses new ones
// (a severed link), Resume() restores forwarding. It exists so a test can drop and restore a connector's
// SMSC link at a STABLE address — unlike stopping the simulator container, whose mapped port may change
// on restart while the pool holds a static bind address. Safe for concurrent use.
package tcpproxy

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Proxy relays TCP from its own listen address to a fixed upstream, with a test-controlled cut/resume.
type Proxy struct {
	ln       net.Listener
	upstream string

	mu     sync.Mutex
	paused bool
	conns  map[net.Conn]struct{} // every live client+upstream conn, closed on Cut

	wg sync.WaitGroup
}

// New starts a proxy forwarding to upstream, listening on an ephemeral loopback port, and registers its
// shutdown via t.Cleanup. It fails the test if it cannot listen.
func New(t *testing.T, upstream string) *Proxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("tcpproxy: listen: %v", err)
	}
	p := &Proxy{ln: ln, upstream: upstream, conns: make(map[net.Conn]struct{})}
	p.wg.Add(1)
	go p.accept()
	t.Cleanup(p.Close)
	return p
}

// Addr is the proxy's listen address — the connector binds this, not the upstream, so Cut/Resume can
// sever and restore the link without the address ever changing.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Cut severs the link: it closes every live connection (in-flight binds see their socket drop) and marks
// the proxy paused so new dials are accepted then immediately closed (a refused/dead SMSC). Idempotent.
func (p *Proxy) Cut() {
	p.mu.Lock()
	p.paused = true
	live := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		live = append(live, c)
	}
	p.mu.Unlock()
	for _, c := range live {
		_ = c.Close()
	}
}

// Resume restores forwarding for connections dialled after the call. Idempotent.
func (p *Proxy) Resume() {
	p.mu.Lock()
	p.paused = false
	p.mu.Unlock()
}

// Close stops the proxy: it closes the listener, cuts all live connections and joins every goroutine.
// Invoked automatically via t.Cleanup; safe to call more than once.
func (p *Proxy) Close() {
	_ = p.ln.Close()
	p.Cut()
	p.wg.Wait()
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return // listener closed by Close
		}
		p.mu.Lock()
		paused := p.paused
		p.mu.Unlock()
		if paused {
			_ = client.Close() // link is cut: accept then drop, so a dial "succeeds" but the bind fails
			continue
		}
		p.handle(client)
	}
}

// handle dials the upstream and pumps both directions, tearing both conns down when either ends (a
// normal close or a Cut). Both conns are tracked so Cut can reach them.
func (p *Proxy) handle(client net.Conn) {
	upstream, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(context.Background(), "tcp", p.upstream)
	if err != nil {
		_ = client.Close()
		return
	}
	// Track the pair and re-check paused under the SAME lock, closing the TOCTOU window with Cut: either
	// Cut already snapshotted these conns (added before its snapshot) and will close them, or Cut set
	// paused before this lock and we close them here — a connection dialled after a Cut never relays.
	p.mu.Lock()
	if p.paused {
		p.mu.Unlock()
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	p.conns[client] = struct{}{}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()

	done := make(chan struct{}, 2)
	pump := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	p.wg.Add(3)
	go func() { defer p.wg.Done(); pump(upstream, client) }()
	go func() { defer p.wg.Done(); pump(client, upstream) }()
	go func() {
		defer p.wg.Done()
		<-done // one side finished (EOF or a Cut close) → tear the pair down
		p.remove(client)
		p.remove(upstream)
		_ = client.Close()
		_ = upstream.Close()
	}()
}

func (p *Proxy) remove(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}
