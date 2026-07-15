package e164_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/platform/e164"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		region string
		want   string
	}{
		{"already e164", "+2250700000000", "CI", "+2250700000000"},
		{"e164 ignores region", "+33612345678", "CI", "+33612345678"},
		{"national with default region", "0700000000", "CI", "+2250700000000"},
		{"french national", "0612345678", "FR", "+33612345678"},
		{"spaces stripped", "+225 07 00 00 00 00", "CI", "+2250700000000"},
		{"dashes stripped", "+225-07-00-00-00-00", "CI", "+2250700000000"},
		{"parentheses stripped", "+33 (0)6 12 34 56 78", "FR", "+33612345678"},
		{"dots stripped", "06.12.34.56.78", "FR", "+33612345678"},
		{"surrounding whitespace", "  +2250700000000  ", "CI", "+2250700000000"},
		{"lowercase region", "0700000000", "ci", "+2250700000000"},
		{"international 00 prefix", "002250700000000", "CI", "+2250700000000"},
		{"empty region with e164", "+2250700000000", "", "+2250700000000"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := e164.Normalize(tc.raw, tc.region)
			if err != nil {
				t.Fatalf("Normalize(%q, %q) error = %v", tc.raw, tc.region, err)
			}
			if got != tc.want {
				t.Errorf("Normalize(%q, %q) = %q, want %q", tc.raw, tc.region, got, tc.want)
			}
		})
	}
}

// TestNormalizeIsIdempotent pins the property downstream state depends on: normalizing an
// already-normalized number must be a no-op, or opt-out and route keys would drift.
func TestNormalizeIsIdempotent(t *testing.T) {
	for _, raw := range []string{"0700000000", "+225 07 00 00 00 00", "+2250700000000"} {
		once, err := e164.Normalize(raw, "CI")
		if err != nil {
			t.Fatalf("Normalize(%q) error = %v", raw, err)
		}
		twice, err := e164.Normalize(once, "CI")
		if err != nil {
			t.Fatalf("Normalize(%q) error = %v", once, err)
		}
		if once != twice {
			t.Errorf("not idempotent for %q: %q then %q", raw, once, twice)
		}
	}
}

// TestNormalizeConvergesOnOneForm is the other half of that contract: every spelling of one
// number must land on a single key.
func TestNormalizeConvergesOnOneForm(t *testing.T) {
	spellings := []string{
		"+2250700000000",
		"0700000000",
		"+225 07 00 00 00 00",
		"+225-07-00-00-00-00",
		"002250700000000",
		"  +2250700000000 ",
	}

	const want = "+2250700000000"
	for _, raw := range spellings {
		got, err := e164.Normalize(raw, "CI")
		if err != nil {
			t.Fatalf("Normalize(%q) error = %v", raw, err)
		}
		if got != want {
			t.Errorf("Normalize(%q) = %q, want %q — spellings must converge", raw, got, want)
		}
	}
}

func TestNormalizeRejects(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		region string
	}{
		{"empty", "", "CI"},
		{"whitespace only", "   ", "CI"},
		{"letters", "not-a-number", "CI"},
		{"too short", "07", "CI"},
		{"national without region", "0700000000", ""},
		{"unknown country code", "+9990700000000", "CI"},
		{"invalid for region", "+225000000000", "CI"},
		{"digits only, no region", "123", ""},
		{"way too long", "+2250700000000000000000000", "CI"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := e164.Normalize(tc.raw, tc.region)
			if err == nil {
				t.Fatalf("Normalize(%q, %q) = %q, want error", tc.raw, tc.region, got)
			}
			if !errors.Is(err, e164.ErrInvalidNumber) {
				t.Errorf("error = %v, want it to wrap ErrInvalidNumber", err)
			}
			if got != "" {
				t.Errorf("Normalize returned %q alongside an error, want empty string", got)
			}
		})
	}
}

// TestNormalizeErrorMentionsInput keeps rejections diagnosable: an operator must be able to see
// which number failed. An MSISDN is an identifier, not message content — safe to surface.
func TestNormalizeErrorMentionsInput(t *testing.T) {
	_, err := e164.Normalize("07", "CI")
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.Contains(err.Error(), "07") {
		t.Errorf("error %q should echo the offending input", err)
	}
}

func TestIsValid(t *testing.T) {
	tests := []struct {
		raw    string
		region string
		want   bool
	}{
		{"+2250700000000", "CI", true},
		{"0700000000", "CI", true},
		{"not-a-number", "CI", false},
		{"", "CI", false},
		{"0700000000", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.raw+"/"+tc.region, func(t *testing.T) {
			if got := e164.IsValid(tc.raw, tc.region); got != tc.want {
				t.Errorf("IsValid(%q, %q) = %v, want %v", tc.raw, tc.region, got, tc.want)
			}
		})
	}
}

func FuzzNormalizeNeverPanics(f *testing.F) {
	seeds := []string{"+2250700000000", "0700000000", "", "+", "00", "not-a-number", "+225 07 00 00 00 00"}
	for _, s := range seeds {
		f.Add(s, "CI")
	}

	f.Fuzz(func(t *testing.T, raw, region string) {
		got, err := e164.Normalize(raw, region)
		if err != nil {
			return
		}
		// A successful normalization must always yield canonical E.164.
		if !strings.HasPrefix(got, "+") {
			t.Errorf("Normalize(%q, %q) = %q, want a leading +", raw, region, got)
		}
		if len(got) < 2 || len(got) > 16 {
			t.Errorf("Normalize(%q, %q) = %q, length %d outside E.164 bounds", raw, region, got, len(got))
		}
		for _, r := range got[1:] {
			if r < '0' || r > '9' {
				t.Errorf("Normalize(%q, %q) = %q, contains non-digit %q", raw, region, got, r)
			}
		}
	})
}
