// Package tlstest issues the certificates the TLS tests need, in memory and on the fly. Nothing is
// committed: a private key in a repository is a secret of that repository, and a fixture certificate
// expires one day in the middle of a green CI run.
//
// The CA is per-call, so two CAs in one test are two calls — which is how a test proves that a peer
// signed by somebody else is refused.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA is a throwaway certificate authority and the directory its PEM files live in.
type CA struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	dir    string
	CAFile string
}

// NewCA issues a self-signed authority under t.TempDir() and returns it. The file it writes is the
// ca.crt a server verifies its clients against, and a client its server.
func NewCA(t *testing.T) *CA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: "tlstest-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("self-sign the CA: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse the CA: %v", err)
	}

	ca := &CA{cert: cert, key: key, dir: t.TempDir()}
	ca.CAFile = filepath.Join(ca.dir, "ca.crt")
	writeFile(t, ca.CAFile, "CERTIFICATE", der)
	return ca
}

// Issue signs a leaf for the given DNS names, usable as both a server and a client certificate — as
// every pod of this repository is: each one both serves and calls. It returns the paths of the
// certificate and of its key, under the CA's own directory so a rotation can overwrite them.
func (ca *CA) Issue(t *testing.T, name string, dnsNames ...string) (certFile, keyFile string) {
	t.Helper()
	certFile = filepath.Join(ca.dir, name+".crt")
	keyFile = filepath.Join(ca.dir, name+".key")
	ca.IssueInto(t, certFile, keyFile, dnsNames...)
	return certFile, keyFile
}

// IssueInto signs a leaf into the given paths, overwriting them. A rotation is exactly that: the same
// paths, new bytes, which is what the kubelet does when the Secret changes.
func (ca *CA) IssueInto(t *testing.T, certFile, keyFile string, dnsNames ...string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(t),
		Subject:      pkix.Name{CommonName: dnsNames[0]},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign the leaf: %v", err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}

	writeFile(t, certFile, "CERTIFICATE", der)
	writeFile(t, keyFile, "PRIVATE KEY", pkcs8)
}

func serial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("draw a serial: %v", err)
	}
	return n
}

func writeFile(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
