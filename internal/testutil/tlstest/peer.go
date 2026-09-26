package tlstest

import (
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// HandshakePeer listens as a peer signed by ca under the name localhost, and signals each TLS handshake that
// completes. It speaks no application protocol, so the client's request fails afterwards: it proves the
// transport, which is all a store's TLS option changes. addr names localhost, so the client derives the
// ServerName the certificate carries.
func (ca *CA) HandshakePeer(t *testing.T) (addr string, handshakes <-chan struct{}) {
	t.Helper()
	certFile, keyFile := ca.Issue(t, "peer", "localhost")
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load peer pair: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	done := make(chan struct{}, 16)
	go func() {
		for {
			raw, err := lis.Accept()
			if err != nil {
				return
			}
			conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}})
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if conn.Handshake() == nil {
				select {
				case done <- struct{}{}:
				default:
				}
			}
			_ = conn.Close()
		}
	}()
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return net.JoinHostPort("localhost", port), done
}
