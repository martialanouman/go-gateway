package main

import (
	"crypto/tls"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
)

// TestGeneratedFilesCompleteAMutualHandshake is the only assertion this tool needs, and the one an
// operator actually cares about: the files it writes are usable by the code that will read them. A
// generator that produced a certificate without SANs, or a key that does not match, would fail in a
// cluster with a handshake error that names neither.
func TestGeneratedFilesCompleteAMutualHandshake(t *testing.T) {
	dir := t.TempDir()
	if err := run(dir, "gateway", "content-key-svc, router-svc", time.Hour); err != nil {
		t.Fatalf("run: %v", err)
	}

	files := func(svc string) tlsconf.Files {
		return tlsconf.Files{
			Cert:     filepath.Join(dir, svc+".crt"),
			Key:      filepath.Join(dir, svc+".key"),
			ClientCA: filepath.Join(dir, "ca.crt"),
		}
	}

	// The server names its caller, so the SANs the tool writes are load-bearing rather than decorative.
	serverCfg, err := files("content-key-svc").ServerConfig([]string{"router-svc"})
	if err != nil {
		t.Fatalf("ServerConfig: %v", err)
	}
	clientCfg, err := files("router-svc").ClientConfig()
	if err != nil {
		t.Fatalf("ClientConfig: %v", err)
	}
	clientCfg.ServerName = "content-key-svc"

	lis, err := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = lis.Close() }()
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = io.Copy(conn, conn)
	}()

	conn, err := tls.Dial("tcp", lis.Addr().(*net.TCPAddr).String(), clientCfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// TestServicesIsRequired: writing a CA and nothing else looks like success and is not.
func TestServicesIsRequired(t *testing.T) {
	if err := run(t.TempDir(), "gateway", "  ,  ", time.Hour); err == nil {
		t.Fatal("run() accepted an empty service list")
	}
}
