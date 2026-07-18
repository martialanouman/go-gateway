package e164_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/platform/e164"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"already e164", "+2250700000000", "2250700000000"},
		{"e164 french", "+33612345678", "33612345678"},
		{"no plus, full country code", "2250700000000", "2250700000000"},
		{"no plus french", "33612345678", "33612345678"},
		{"spaces stripped", "+225 07 00 00 00 00", "2250700000000"},
		{"dashes stripped", "+225-07-00-00-00-00", "2250700000000"},
		{"parentheses stripped", "+33 (6) 12 34 56 78", "33612345678"},
		{"surrounding whitespace", "  +2250700000000  ", "2250700000000"},
		{"no plus with spaces", "225 07 00 00 00 00", "2250700000000"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := e164.Normalize(tc.raw)
			if err != nil {
				t.Fatalf("Normalize(%q) error = %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("Normalize(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// TestNormalizeIsIdempotent pins the property downstream state depends on: normalizing an
// already-normalized number must be a no-op, or opt-out and route keys would drift.
func TestNormalizeIsIdempotent(t *testing.T) {
	for _, raw := range []string{"2250700000000", "+225 07 00 00 00 00", "+2250700000000"} {
		once, err := e164.Normalize(raw)
		if err != nil {
			t.Fatalf("Normalize(%q) error = %v", raw, err)
		}
		twice, err := e164.Normalize(once)
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
		"2250700000000",
		"+225 07 00 00 00 00",
		"+225-07-00-00-00-00",
		"  +2250700000000 ",
	}

	const want = "2250700000000"
	for _, raw := range spellings {
		got, err := e164.Normalize(raw)
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
		name string
		raw  string
	}{
		{"empty", ""},
		{"whitespace only", "   "},
		{"letters", "not-a-number"},
		{"too short", "07"},
		{"national without country code", "0700000000"},
		{"unknown country code", "9990700000000"},
		{"invalid for country code", "225000000000"},
		{"digits too short", "123"},
		{"way too long", "2250700000000000000000000"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := e164.Normalize(tc.raw)
			if err == nil {
				t.Fatalf("Normalize(%q) = %q, want error", tc.raw, got)
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
	_, err := e164.Normalize("07")
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.Contains(err.Error(), "07") {
		t.Errorf("error %q should echo the offending input", err)
	}
}

func TestIsValid(t *testing.T) {
	tests := []struct {
		raw  string
		want bool
	}{
		{"+2250700000000", true},
		{"2250700000000", true},
		{"not-a-number", false},
		{"", false},
		{"0700000000", false},
	}

	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			if got := e164.IsValid(tc.raw); got != tc.want {
				t.Errorf("IsValid(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func FuzzNormalizeNeverPanics(f *testing.F) {
	seeds := []string{"+2250700000000", "2250700000000", "", "+", "00", "not-a-number", "+225 07 00 00 00 00"}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		got, err := e164.Normalize(raw)
		if err != nil {
			return
		}
		// A successful normalization must always yield canonical digits-only E.164 (no leading +).
		if len(got) < 1 || len(got) > 15 {
			t.Errorf("Normalize(%q) = %q, length %d outside E.164 bounds", raw, got, len(got))
		}
		for _, r := range got {
			if r < '0' || r > '9' {
				t.Errorf("Normalize(%q) = %q, contains non-digit %q", raw, got, r)
			}
		}
	})
}
