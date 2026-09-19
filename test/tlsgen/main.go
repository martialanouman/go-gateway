// Command tlsgen issues a throwaway certificate authority and one leaf per service, for a test cluster
// that has no cert-manager. With cert-manager, use deploy/k8s/tls/ instead and let it rotate them.
//
// It lives under test/ rather than cmd/ because it is a tool, not a service — the same place as
// test/load/bindgen and test/load/stub, which are run with go run and ship in no image.
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

// writeSecret writes a PEM file and enforces its mode, which WriteFile does not do on an existing file.
func writeSecret(path string, content []byte) error {
	if err := os.WriteFile(path, content, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("tighten %s: %w", path, err)
	}
	return nil
}

func run(out, ns, services string, lifetime time.Duration) error {
	names := splitNames(services)
	if len(names) == 0 {
		return fmt.Errorf("-services is required (comma-separated)")
	}
	if err := os.MkdirAll(out, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", out, err)
	}
	// MkdirAll leaves an existing directory's mode alone, and WriteFile keeps an existing file's — so a
	// second run into a directory someone created by hand would silently keep 0755.
	//nolint:gosec // G302: 0700 is the tightest mode a DIRECTORY can have and still be entered; the rule
	// reads it as a file mode. The keys inside are written 0600.
	if err := os.Chmod(out, 0o700); err != nil {
		return fmt.Errorf("tighten %s: %w", out, err)
	}

	auth, err := tlstest.NewAuthority("gateway-ca", lifetime)
	if err != nil {
		return err
	}
	if err := writeSecret(filepath.Join(out, "ca.crt"), auth.CAPEM); err != nil {
		return err
	}

	for _, name := range names {
		// Both the bare name and the in-cluster form: a caller inside the namespace dials the first, a
		// caller in another namespace the second, and a certificate carrying only one refuses the other.
		certPEM, keyPEM, err := auth.Issue(name, []string{name, name + "." + ns + ".svc"})
		if err != nil {
			return err
		}
		if err := writeSecret(filepath.Join(out, name+".crt"), certPEM); err != nil {
			return err
		}
		if err := writeSecret(filepath.Join(out, name+".key"), keyPEM); err != nil {
			return err
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
