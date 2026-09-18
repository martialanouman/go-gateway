// Package tlstest issues the certificates the TLS tests need, in memory and on the fly. Nothing is
// committed: a private key in a repository is a secret of that repository, and a fixture certificate
// expires one day in the middle of a green CI run.
//
// The CA is per-call, so two CAs in one test are two calls — which is how a test proves that a peer
// signed by somebody else is refused.
//
// The Authority half carries no *testing.T, so the same x509 mechanics serve test/tlsgen, the tool that
// seeds a cluster without cert-manager. The SANs are what makes a certificate usable here, and hand-made
// openssl invocations are where they get forgotten.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// DefaultLifetime is how long an issued certificate lives. Short enough that a forgotten test cluster
// stops rather than lingers, long enough to survive a working quarter.
const DefaultLifetime = 90 * 24 * time.Hour

// Authority is a certificate authority: its certificate, its key, and the PEM a peer verifies against.
type Authority struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	CAPEM  []byte
	Expiry time.Duration
}

// NewAuthority creates a self-signed authority. Expiry defaults to DefaultLifetime.
func NewAuthority(commonName string, expiry time.Duration) (*Authority, error) {
	if expiry <= 0 {
		expiry = DefaultLifetime
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate the CA key: %w", err)
	}
	sn, err := serialNumber()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          sn,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(expiry),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("self-sign the CA: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse the CA: %w", err)
	}
	return &Authority{
		cert:   cert,
		key:    key,
		CAPEM:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		Expiry: expiry,
	}, nil
}

// Issue signs a leaf for dnsNames, usable as both a server and a client certificate — as every pod of
// this repository is, since each one both serves and calls.
//
// commonName is deliberately NOT one of the DNS names: identity is read from the SANs, the CN being
// deprecated for that, and a certificate where the two agree cannot tell one check from the other.
func (a *Authority) Issue(commonName string, dnsNames []string) (certPEM, keyPEM []byte, err error) {
	return a.IssueFor(commonName, dnsNames, a.Expiry)
}

// IssueFor signs a leaf with an explicit lifetime. A negative one yields an already-expired certificate,
// which is the only way a test can exercise what this repository does with one.
func (a *Authority) IssueFor(commonName string, dnsNames []string, life time.Duration) (certPEM, keyPEM []byte, err error) {
	if len(dnsNames) == 0 {
		return nil, nil, fmt.Errorf("issue %q: a certificate with no SAN identifies nothing", commonName)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate the leaf key: %w", err)
	}
	sn, err := serialNumber()
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: sn,
		Subject:      pkix.Name{CommonName: commonName},
		DNSNames:     dnsNames,
		// Two hours back, not one: IssueFor takes a NEGATIVE life to produce an expired certificate, and
		// a NotBefore of -1h would then equal NotAfter — a validity window of zero, which reads as
		// malformed rather than as expired.
		NotBefore:   time.Now().Add(-2 * time.Hour),
		NotAfter:    time.Now().Add(life),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return nil, nil, fmt.Errorf("sign the leaf: %w", err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal the leaf key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}), nil
}

func serialNumber() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("draw a serial: %w", err)
	}
	return n, nil
}

// CA is an Authority with its files on disk, for a test that needs paths rather than bytes.
type CA struct {
	auth   *Authority
	dir    string
	CAFile string
}

// IssueFor signs a leaf with a lifetime of its own — negative for one that already expired, short for
// one about to. It takes the duration as an argument rather than moving the authority's own Expiry,
// which would be a data race the day two parallel subtests shared a CA.
func (ca *CA) IssueFor(t *testing.T, life time.Duration, name string, dnsNames ...string) (certFile, keyFile string) {
	t.Helper()
	certFile = filepath.Join(ca.dir, name+".crt")
	keyFile = filepath.Join(ca.dir, name+".key")
	certPEM, keyPEM, err := ca.auth.IssueFor(name, dnsNames, life)
	if err != nil {
		t.Fatalf("issue %s: %v", name, err)
	}
	writeBoth(t, certFile, keyFile, certPEM, keyPEM)
	return certFile, keyFile
}

// NewCA issues a throwaway authority under t.TempDir() and writes its ca.crt.
func NewCA(t *testing.T) *CA {
	t.Helper()
	// DefaultLifetime, not an hour: tlsconf warns about a certificate expiring within a week, and a
	// fixture that tripped that warning would make every test read its own noise.
	auth, err := NewAuthority("tlstest-ca", DefaultLifetime)
	if err != nil {
		t.Fatalf("new authority: %v", err)
	}
	ca := &CA{auth: auth, dir: t.TempDir()}
	ca.CAFile = filepath.Join(ca.dir, "ca.crt")
	if err := os.WriteFile(ca.CAFile, auth.CAPEM, 0o600); err != nil {
		t.Fatalf("write the CA: %v", err)
	}
	return ca
}

// Issue signs a leaf named name for dnsNames and returns the paths it wrote, under the CA's directory
// so a rotation can overwrite them.
func (ca *CA) Issue(t *testing.T, name string, dnsNames ...string) (certFile, keyFile string) {
	t.Helper()
	certFile = filepath.Join(ca.dir, name+".crt")
	keyFile = filepath.Join(ca.dir, name+".key")
	ca.issue(t, name, certFile, keyFile, dnsNames)
	return certFile, keyFile
}

// IssueInto signs a leaf into the given paths, overwriting them. A rotation is exactly that: the same
// paths, new bytes, which is what the kubelet does when the Secret changes.
func (ca *CA) IssueInto(t *testing.T, certFile, keyFile string, dnsNames ...string) {
	t.Helper()
	ca.issue(t, filepath.Base(certFile), certFile, keyFile, dnsNames)
}

func (ca *CA) issue(t *testing.T, name, certFile, keyFile string, dnsNames []string) {
	t.Helper()
	certPEM, keyPEM, err := ca.auth.Issue(name, dnsNames)
	if err != nil {
		t.Fatalf("issue %s: %v", name, err)
	}
	writeBoth(t, certFile, keyFile, certPEM, keyPEM)
}

func writeBoth(t *testing.T, certFile, keyFile string, certPEM, keyPEM []byte) {
	t.Helper()
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write %s: %v", certFile, err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write %s: %v", keyFile, err)
	}
}
