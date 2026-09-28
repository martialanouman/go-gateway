// Command test-env seeds the test environment's control plane and smoke-tests its SMPP path,
// run as Kubernetes Jobs against the Admin API and the SMPP server (step-275).
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	adminURL        = "https://admin-api-svc:8081/v1/admin"
	adminServerName = "admin-api-svc"
	smppAddr        = "smpp-server-svc:2775"
	smppServerName  = "smpp-server-svc"
	connectorHost   = "smsc-simulator"
	connectorPort   = 2775
	deadline        = 3 * time.Minute
	smokeRetry      = 2 * time.Second
	dlrWait         = 60 * time.Second
)

func main() {
	log.SetFlags(0)
	if len(os.Args) != 2 || (os.Args[1] != "seed" && os.Args[1] != "smoke") {
		fmt.Fprintln(os.Stderr, "usage: test-env seed|smoke")
		os.Exit(2)
	}
	cmd := os.Args[1]
	if err := run(cmd); err != nil {
		//nolint:gosec // G706: cmd is checked above to be exactly "seed" or "smoke", never raw input.
		log.Fatalf("%s : %v", cmd, err)
	}
}

func run(cmd string) error {
	tlsCfg, err := loadTLSConfig(mustEnv("TLS_DIR"))
	if err != nil {
		return fmt.Errorf("configuration TLS : %w", err)
	}

	token, err := adminToken(os.Getenv("HTTP_ADMIN_TOKENS"))
	if err != nil {
		return fmt.Errorf("HTTP_ADMIN_TOKENS : %w", err)
	}
	a := &admin{
		base:  adminURL,
		token: token,
		hc: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{
				RootCAs:      tlsCfg.RootCAs,
				Certificates: tlsCfg.Certificates,
				ServerName:   adminServerName,
				MinVersion:   tls.VersionTLS13,
			}},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	switch cmd {
	case "seed":
		id, err := seed(ctx, a, connectorSpec{
			Host:     connectorHost,
			Port:     connectorPort,
			SystemID: mustEnv("CONNECTOR_SYSTEM_ID"),
			Password: mustEnv("CONNECTOR_PASSWORD"),
		})
		if err != nil {
			return err
		}
		fmt.Printf("connector_id=%s\n", id)
		return nil
	case "smoke":
		dialer := tls.Dialer{Config: &tls.Config{
			RootCAs:    tlsCfg.RootCAs,
			ServerName: smppServerName,
			MinVersion: tls.VersionTLS12,
		}}
		dial := func(ctx context.Context) (net.Conn, error) { return dialer.DialContext(ctx, "tcp", smppAddr) }
		return smoke(ctx, a, dial, smokeRetry, dlrWait)
	}
	// unreachable: main validates cmd against exactly "seed"/"smoke" before calling run.
	return fmt.Errorf("sous-commande %q inconnue", cmd)
}

type tlsAssets struct {
	RootCAs      *x509.CertPool
	Certificates []tls.Certificate
}

func loadTLSConfig(dir string) (tlsAssets, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
	if err != nil {
		return tlsAssets{}, fmt.Errorf("certificat client : %w", err)
	}
	//nolint:gosec // G304: dir is TLS_DIR, an operator-set Job env var, not untrusted input.
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return tlsAssets{}, fmt.Errorf("CA : %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return tlsAssets{}, fmt.Errorf("CA : PEM invalide dans %s", filepath.Join(dir, "ca.crt"))
	}
	return tlsAssets{RootCAs: pool, Certificates: []tls.Certificate{cert}}, nil
}

func adminToken(tokens string) (string, error) {
	first, _, _ := strings.Cut(tokens, ",")
	token, _, _ := strings.Cut(first, ":")
	if token == "" {
		return "", errors.New("aucun jeton avant le premier ':'")
	}
	return token, nil
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s : requis", key)
	}
	return v
}
