package adminapi

import (
	"context"
	"strings"

	"github.com/martialanouman/go-gateway/internal/auth"
)

// msisdnHead is how many leading digits stay visible: enough to keep the country and operator prefix, which
// is what an operator reasons about, without identifying a subscriber.
const msisdnHead = 4

// msisdnTail is how many trailing digits stay visible. Two is enough to tell neighbouring numbers apart when
// correlating complaints, and far too few to reconstruct one.
const msisdnTail = 2

// maskMSISDN redacts a subscriber number unless the caller may see it (step-186 rule, shared by the trace,
// the search and the export so one change moves all three).
func maskMSISDN(value string, reveal bool) string {
	if reveal || value == "" {
		return value
	}
	if len(value) <= msisdnHead+msisdnTail {
		// Too short to show a prefix without showing most of it.
		if len(value) <= msisdnTail {
			return strings.Repeat("*", len(value))
		}
		return strings.Repeat("*", len(value)-msisdnTail) + value[len(value)-msisdnTail:]
	}
	return value[:msisdnHead] +
		strings.Repeat("*", len(value)-msisdnHead-msisdnTail) +
		value[len(value)-msisdnTail:]
}

// maskAddresses masks the SUBSCRIBER side of a message, which is the only side that identifies a person.
//
// On an MT the subscriber is the destination and the source is a sender ID — often alphanumeric, so masking
// it gains no privacy and loses the most diagnostic field. On an MO it is the reverse: the source is the
// subscriber, the destination is the operator's own inbound number. An unrecognised direction masks both.
func maskAddresses(direction, source, dest string, reveal bool) (maskedSource, maskedDest string) {
	switch direction {
	case "mt":
		return source, maskMSISDN(dest, reveal)
	case "mo":
		return maskMSISDN(source, reveal), dest
	default:
		return maskMSISDN(source, reveal), maskMSISDN(dest, reveal)
	}
}

// mayRevealMSISDN reports whether the caller holds the reveal scope.
//
// Deliberately NOT content:read: reading a body is a strictly greater act than seeing a destination number,
// so gating numbers behind it would force anyone who needs a number to also be able to decrypt messages.
func mayRevealMSISDN(ctx context.Context) bool {
	p, ok := auth.PrincipalFrom(ctx)
	return ok && p.Has(auth.ScopeMSISDNReveal)
}
