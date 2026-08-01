package smppserver

import (
	"fmt"
	"net"
	"time"

	proxyproto "github.com/pires/go-proxyproto"
)

// proxyHeaderTimeout bounds how long a connection may take to send its PROXY header. A peer that opens a
// socket and then says nothing must not hold an accept slot indefinitely (slow-loris).
const proxyHeaderTimeout = 3 * time.Second

// wrapProxyProtocol returns lis decorated to read a PROXY protocol header (v1 and v2), so the throttle and
// the security logs see the ESME's real address instead of the load balancer's. With no trusted range it
// returns lis UNCHANGED: a deployment without a balancer must keep the transport peer address and must not
// be trickable by a header a client sends on its own.
//
// The trusted ranges are what makes this safe, and they are not optional decoration. The library's default
// REQUIRE policy only mandates that a header be present — it still believes a header from ANY peer. On a
// listener reachable by clients, that would let one forge a source address to escape its own bind throttle
// or to poison another client's counter. Restricting acceptance to the balancer's ranges closes that:
// outside them the header is ignored and the real peer address stands.
//
// A malformed range is a startup error rather than a silent fallback — a typo in a manifest must not
// quietly degrade to the very self-DoS this exists to prevent (see remoteIP).
func wrapProxyProtocol(lis net.Listener, trustedCIDRs []string) (net.Listener, error) {
	if len(trustedCIDRs) == 0 {
		return lis, nil
	}

	// Validate each range here rather than leaning on the library: a bare IP ("10.0.0.1") is a plausible
	// manifest typo, and it must fail loudly instead of narrowing the trusted set by surprise.
	for _, c := range trustedCIDRs {
		if _, _, err := net.ParseCIDR(c); err != nil {
			return nil, fmt.Errorf("smppserver: trusted proxy range %q: %w", c, err)
		}
	}

	policy, err := proxyproto.TrustProxyHeaderFromRanges(trustedCIDRs)
	if err != nil {
		return nil, fmt.Errorf("smppserver: build proxy policy: %w", err)
	}
	return &proxyproto.Listener{
		Listener:          lis,
		ConnPolicy:        policy,
		ReadHeaderTimeout: proxyHeaderTimeout,
	}, nil
}
