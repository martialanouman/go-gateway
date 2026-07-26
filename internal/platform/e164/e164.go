// Package e164 normalizes MSISDNs to a canonical, digits-only international form.
//
// Normalization runs early in the MT pipeline (spec §6.19) and its output is the key every later
// stage keys on — opt-out lookups, exact-number routes, suppression Bloom filters. Two spellings
// of one number that normalize differently would silently split that state, so the canonical form
// produced here is a contract, not a convenience.
package e164

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nyaruka/phonenumbers"
)

// ErrInvalidNumber marks a number that cannot be normalized. Callers map it to the error code
// matching the field's direction — invalid_destination or invalid_source (guide d'ingénierie
// §11.3) — since this package cannot know which side it was handed.
var ErrInvalidNumber = errors.New("invalid e164 number")

// Normalize parses raw and returns its canonical international form as digits only, with no leading
// "+" ("2250700000000"). The public contract makes the "+" optional (openapi-public.yaml `to`/`from`
// patterns `^\+?[1-9]…`), so a number may arrive with or without it; either way it must carry its
// country code, since no default region is assumed. The stored form drops the "+": every later stage
// — opt-out, routing, the CDR, the SMSC destination address — keys on these digits.
//
// A number that parses but is not valid (wrong length for its country code, unassigned country code)
// is rejected: possible-but-invalid numbers reach no handset, and letting them through would only
// defer the failure to the SMSC.
//
// Every failure wraps ErrInvalidNumber. The returned error never contains the parse internals of the
// upstream library beyond its message, and raw is echoed back for operator diagnosis — an MSISDN is
// an identifier, not message content, so it is safe to surface here.
func Normalize(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("normalize %q: empty number: %w", raw, ErrInvalidNumber)
	}

	// The "+" is optional on the wire but required for region-less parsing: a bare international
	// number ("2250700000000") is unparseable under UNKNOWN_REGION until it is marked international.
	candidate := trimmed
	if !strings.HasPrefix(candidate, "+") {
		candidate = "+" + candidate
	}

	num, err := phonenumbers.Parse(candidate, phonenumbers.UNKNOWN_REGION)
	if err != nil {
		return "", fmt.Errorf("normalize %q: %v: %w", raw, err, ErrInvalidNumber)
	}
	if !phonenumbers.IsValidNumber(num) {
		return "", fmt.Errorf("normalize %q: not a valid number: %w", raw, ErrInvalidNumber)
	}

	// Canonical E.164 minus the "+": the stored and keyed form is digits only.
	return strings.TrimPrefix(phonenumbers.Format(num, phonenumbers.E164), "+"), nil
}

// IsValid reports whether raw normalizes. Use it for a predicate; use Normalize when the canonical
// form is needed (which is nearly always).
func IsValid(raw string) bool {
	_, err := Normalize(raw)
	return err == nil
}

// NormalizeAddr canonicalizes a sender/inbound address for keying, tolerating non-E.164 addresses.
// A valid MSISDN yields its canonical digits-only form; a short code or alphanumeric address (which
// does not parse as E.164) falls back to its trimmed self. Use this — not Normalize — when the value
// may be a shortcode/alphanumeric (inbound-number and opt-out inbound_number-scope keys), so both
// sides of a lookup agree on the same key.
func NormalizeAddr(raw string) string {
	if n, err := Normalize(raw); err == nil {
		return n
	}
	return strings.TrimSpace(raw)
}
