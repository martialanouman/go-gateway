// Package tlsconf builds the TLS configuration of every surface this repository serves or calls, from
// three PEM files on disk. It knows nothing about who writes them: cert-manager, an internal PKI or an
// operator's kubectl all look alike from here.
//
// The files are read at every handshake, not once at boot, because certificates rotate under a running
// process — the kubelet rewrites a Secret volume in place and the pod never restarts. What makes that
// work on the serving side is GetConfigForClient rather than GetCertificate: ClientCAs is read once at
// the start of a handshake and no callback refreshes it, so a CA pool installed at boot would never
// move. GetConfigForClient hands back a whole config per handshake, so the leaf and the CA rotate
// together.
//
// The dialling side has no equivalent, and crypto/tls offers none: RootCAs lives in the config handed
// once to the transport. The leaf rotates through GetClientCertificate; the CA does not, and a CA change
// needs the clients restarted. That is a deliberate limit (step-300): neither of the two workarounds
// re-verifies connections ALREADY established, so neither evicts a compromised peer — only a restart
// does, and it is both faster than the kubelet's resync and auditable.
package tlsconf

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
)

// Files are the three PEM paths that carry a pod's TLS identity: its certificate, its private key, and
// the authority it verifies its peers against. One identity per pod, not one per surface — a service
// listens and calls with the same certificate.
type Files struct {
	Cert, Key, ClientCA string

	// Logger receives the one warning this package emits, when ClientCA changes under a running
	// process. Nil falls back to slog.Default().
	Logger *slog.Logger
}

// ServerConfig builds the listening side: mutual TLS, verified against ClientCA.
//
// allowedClients is the identity check on top of the chain check, and the two answer different
// questions: a certificate from our CA proves the peer is one of our pods, never WHICH one. A server
// that hands out something only some callers may have — content-key-svc hands out a customer's data key
// — names them here. An empty list admits every holder of a certificate from our CA, which is what the
// servers with no named callers want.
//
// It fails now if the files cannot be read, so a bad path is a boot error and not a handshake that
// starts failing at three in the morning.
func (f Files) ServerConfig(allowedClients []string) (*tls.Config, error) {
	ld := &loader{files: f}
	if _, err := ld.load(); err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// Everything of substance is decided per handshake, so a rotation needs no restart. The outer
		// config exists only to carry this callback.
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			st, err := ld.load()
			if err != nil {
				return nil, err
			}
			return &tls.Config{
				MinVersion:   tls.VersionTLS13,
				Certificates: []tls.Certificate{*st.cert},
				ClientCAs:    st.pool,
				ClientAuth:   tls.RequireAndVerifyClientCert,
				// VerifyConnection, not VerifyPeerCertificate: on a RESUMED session the latter is never
				// called again, so a caller dropped from the allowlist would keep its access for as long
				// as its ticket lives (gosec G123). VerifyConnection runs on both paths, and the state it
				// receives carries the chains the CA vouched for either way.
				VerifyConnection: allowlist(allowedClients),
			}, nil
		},
	}, nil
}

// ClientConfig builds the dialling side. The caller sets ServerName, which is the peer it means to
// reach; nothing here can know it.
func (f Files) ClientConfig() (*tls.Config, error) {
	ld := &loader{files: f}
	st, err := ld.load()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// RootCAs is frozen here, and deliberately: see the package doc.
		RootCAs: st.pool,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			st, err := ld.load()
			if err != nil {
				return nil, err
			}
			return st.cert, nil
		},
	}, nil
}

// state is one loaded generation of the three files.
type state struct {
	cert *tls.Certificate
	pool *x509.CertPool
}

// loader caches a generation and reloads it when the files change on disk. The cache key is the
// (modification time, size) of each file rather than the modification time alone: two writes can land
// in the same clock tick, and a rotation missed that way would read as a success.
type loader struct {
	files Files

	mu    sync.RWMutex
	stamp string
	cur   *state
	// caSum is the digest of the CA read the first time, and warned whether the change has been
	// announced — once, not at every handshake.
	caSum   [sha256.Size]byte
	haveSum bool
	warned  bool
}

func (l *loader) load() (*state, error) {
	stamp, err := l.stamp3()
	if err != nil {
		return nil, err
	}

	l.mu.RLock()
	if l.cur != nil && l.stamp == stamp {
		defer l.mu.RUnlock()
		return l.cur, nil
	}
	l.mu.RUnlock()

	l.mu.Lock()
	defer l.mu.Unlock()
	// Another goroutine may have reloaded the same generation while this one waited for the lock.
	if l.cur != nil && l.stamp == stamp {
		return l.cur, nil
	}

	cert, err := tls.LoadX509KeyPair(l.files.Cert, l.files.Key)
	if err != nil {
		return nil, fmt.Errorf("tlsconf: load the key pair: %w", err)
	}
	pem, err := os.ReadFile(l.files.ClientCA)
	if err != nil {
		return nil, fmt.Errorf("tlsconf: read the CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tlsconf: %s holds no certificate", l.files.ClientCA)
	}
	l.announceCAChange(sha256.Sum256(pem))

	l.cur = &state{cert: &cert, pool: pool}
	l.stamp = stamp
	return l.cur, nil
}

// stamp3 fingerprints the three files by size and modification time.
func (l *loader) stamp3() (string, error) {
	var b []byte
	for _, p := range []string{l.files.Cert, l.files.Key, l.files.ClientCA} {
		fi, err := os.Stat(p)
		if err != nil {
			return "", fmt.Errorf("tlsconf: stat: %w", err)
		}
		b = fmt.Appendf(b, "%d/%d;", fi.Size(), fi.ModTime().UnixNano())
	}
	return string(b), nil
}

// allowlist returns the peer check for the named clients, or nil when none are named — nil being what
// crypto/tls expects for "no extra check", and what an empty list means here.
//
// It matches DNS SANs, never the Common Name: the CN is deprecated as an identity, and cert-manager
// fills dnsNames naturally from a Certificate's spec.
func allowlist(allowed []string) func(tls.ConnectionState) error {
	if len(allowed) == 0 {
		return nil
	}
	return func(cs tls.ConnectionState) error {
		if len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
			return fmt.Errorf("tlsconf: no verified chain to read an identity from")
		}
		leaf := cs.VerifiedChains[0][0]
		for _, san := range leaf.DNSNames {
			if slices.Contains(allowed, san) {
				return nil
			}
		}
		// Both halves are named on purpose. The client is told nothing useful — TLS answers "bad
		// certificate" and stops — so a refusal that did not say which identity it saw, and which it
		// expected, would be undebuggable. Neither is a secret.
		return fmt.Errorf("tlsconf: client identity %v is not an allowed caller %v", leaf.DNSNames, allowed)
	}
}

// announceCAChange warns the first time the authority file differs from the one read at startup.
//
// It exists because the failure it precedes is silent and misattributed: the dialling side cannot
// refresh RootCAs (see the package doc), so a rotated CA makes new handshakes fail with
// "x509: unknown authority" on a pod nobody deployed. The server side does pick the new pool up, which
// makes the asymmetry worse — half the mesh moves, half does not.
func (l *loader) announceCAChange(sum [sha256.Size]byte) {
	if !l.haveSum {
		l.caSum, l.haveSum = sum, true
		return
	}
	if sum == l.caSum || l.warned {
		return
	}
	l.warned = true
	log := l.files.Logger
	if log == nil {
		log = slog.Default()
	}
	log.Warn("tls: the certificate authority changed on disk; restart this service to dial with it",
		"ca_file", l.files.ClientCA)
}
