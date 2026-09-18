// Command tlsgen issues a throwaway certificate authority and one leaf per service, for a test cluster
// that has no cert-manager. With cert-manager, use deploy/k8s/tls/ instead and let it rotate them.
//
// It lives under test/ rather than cmd/, where a new binary would owe the guard in internal/deploy a
// published image and a GoReleaser entry. It is a tool, not a service.
//
// What it does that an openssl invocation forgets: the SANs. Identity is read from the DNS SANs, never
// the Common Name, so a certificate with the right CN and no SAN is refused by a handshake that will
// not say why.
//
// Usage:
//
//	go run ./test/tlsgen -out .tls -ns gateway -services content-key-svc,router-svc
//
// Then, per service:
//
//	kubectl -n gateway create secret generic <svc>-tls \
//	  --from-file=tls.crt=.tls/<svc>.crt \
//	  --from-file=tls.key=.tls/<svc>.key \
//	  --from-file=ca.crt=.tls/ca.crt
//
// create secret tls takes only the certificate and key, never a ca.crt, hence the generic form with the
// three standard key names — so the Deployment reads the same whether cert-manager issued the Secret or
// this tool did.
//
// The certificates do NOT rotate. They live 90 days by default; a cluster that outlives that runs the
// two commands again.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/martialanouman/go-gateway/internal/testutil/tlstest"
)

func main() {
	out := flag.String("out", ".tls", "directory to write the CA and the leaves into")
	ns := flag.String("ns", "gateway", "Kubernetes namespace, used to build each service's DNS SANs")
	services := flag.String("services", "", "comma-separated service names (required)")
	lifetime := flag.Duration("lifetime", tlstest.DefaultLifetime, "how long the certificates live")
	flag.Parse()

	if err := run(*out, *ns, *services, *lifetime); err != nil {
		log.Fatalf("tlsgen: %v", err)
	}
}

func run(out, ns, services string, lifetime time.Duration) error {
	names := splitNames(services)
	if len(names) == 0 {
		return fmt.Errorf("-services is required (comma-separated)")
	}
	if err := os.MkdirAll(out, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", out, err)
	}

	auth, err := tlstest.NewAuthority("gateway-ca", lifetime)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "ca.crt"), auth.CAPEM, 0o600); err != nil {
		return fmt.Errorf("write the CA: %w", err)
	}

	for _, name := range names {
		// Both the bare name and the in-cluster form: a caller inside the namespace dials the first, a
		// caller in another namespace the second, and a certificate carrying only one refuses the other.
		certPEM, keyPEM, err := auth.Issue(name, []string{name, name + "." + ns + ".svc"})
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(out, name+".crt"), certPEM, 0o600); err != nil {
			return fmt.Errorf("write %s.crt: %w", name, err)
		}
		if err := os.WriteFile(filepath.Join(out, name+".key"), keyPEM, 0o600); err != nil {
			return fmt.Errorf("write %s.key: %w", name, err)
		}
		fmt.Printf("%s: %s.crt %s.key\n", name, name, name)
	}
	fmt.Printf("authority: %s/ca.crt, valid %s\n", out, lifetime)
	return nil
}

func splitNames(csv string) []string {
	var names []string
	for _, n := range strings.Split(csv, ",") {
		if n = strings.TrimSpace(n); n != "" {
			names = append(names, n)
		}
	}
	return names
}
