// Package e164 normalizes MSISDNs to the E.164 canonical form.
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

// ErrInvalidNumber marks a number that cannot be normalized to E.164. Callers map it to the
// error code matching the field's direction — invalid_destination or invalid_source (guide
// d'ingénierie §11.3) — since this package cannot know which side it was handed.
var ErrInvalidNumber = errors.New("invalid e164 number")

// Normalize parses raw and returns its E.164 form ("+2250700000000").
//
// defaultRegion is the ISO 3166-1 alpha-2 region ("CI", "FR") used to resolve numbers written in
// national form; it is ignored when raw already carries a "+" country code. A number that parses
// but is not valid for its region is rejected: possible-but-invalid numbers reach no handset, and
// letting them through would only defer the failure to the SMSC.
//
// Every failure wraps ErrInvalidNumber. The returned error never contains the parse internals of
// the upstream library beyond its message, and raw is echoed back for operator diagnosis — an
// MSISDN is an identifier, not message content, so it is safe to surface here.
func Normalize(raw, defaultRegion string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("normalize %q: empty number: %w", raw, ErrInvalidNumber)
	}

	region := strings.ToUpper(strings.TrimSpace(defaultRegion))
	if region == "" {
		region = phonenumbers.UNKNOWN_REGION
	}

	num, err := phonenumbers.Parse(trimmed, region)
	if err != nil {
		return "", fmt.Errorf("normalize %q: %v: %w", raw, err, ErrInvalidNumber)
	}
	if !phonenumbers.IsValidNumber(num) {
		return "", fmt.Errorf("normalize %q: not a valid number for region %s: %w", raw, region, ErrInvalidNumber)
	}

	return phonenumbers.Format(num, phonenumbers.E164), nil
}

// IsValid reports whether raw normalizes to E.164 under defaultRegion. Use it for a predicate;
// use Normalize when the canonical form is needed (which is nearly always).
func IsValid(raw, defaultRegion string) bool {
	_, err := Normalize(raw, defaultRegion)
	return err == nil
}
