package main

import (
	"crypto/tls"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/internal/platform/tlsconf"
	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

// TestGeneratedFilesCompleteAMutualHandshake is the only assertion this tool needs, and the one an
// operator actually cares about: the files it writes are usable by the code that will read them. A
// generator that produced a certificate without SANs, or a key that does not match, would fail in a
// cluster with a handshake error that names neither.
func TestGeneratedFilesCompleteAMutualHandshake(t *testing.T) {
	dir := t.TempDir()
	if err := run(dir, "gateway", "content-key-svc, router-svc", tlstest.DefaultLifetime); err != nil {
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
	serverCfg, err := files("content-key-svc").ServerConfig(tlsconf.ServerOptions{AllowedClients: []string{"router-svc"}})
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
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}()
		}
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

	// The in-cluster name too, which is what a caller in another namespace dials. A certificate carrying
	// only the bare name would refuse it, and -ns would be a flag that changes nothing.
	crossNamespace := clientCfg.Clone()
	crossNamespace.ServerName = "content-key-svc.gateway.svc"
	conn2, err := tls.Dial("tcp", lis.Addr().(*net.TCPAddr).String(), crossNamespace)
	if err != nil {
		t.Fatalf("dial by the in-cluster name: %v", err)
	}
	_ = conn2.Close()
}

// TestServicesIsRequired: writing a CA and nothing else looks like success and is not.
func TestServicesIsRequired(t *testing.T) {
	dir := t.TempDir()
	err := run(dir, "gateway", "  ,  ", time.Hour)
	if err == nil {
		t.Fatal("run() accepted an empty service list")
	}
	if !strings.Contains(err.Error(), "-services") {
		t.Errorf("error = %v, want it to name the missing flag", err)
	}
	// And it stopped before writing anything: a directory holding a lone authority invites a second run
	// that reuses it, which is not what happened here.
	if _, err := os.Stat(filepath.Join(dir, "ca.crt")); !os.IsNotExist(err) {
		t.Error("an authority was written although the run failed")
	}
}
