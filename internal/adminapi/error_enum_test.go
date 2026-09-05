package adminapi_test

import (
	"slices"
	"testing"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestErrorCodeEnumMatchesTheCatalogue (step-260h) keeps the contract's Error.code enum equal to the
// catalogue's HTTP-surfaced codes, in both directions.
func TestErrorCodeEnumMatchesTheCatalogue(t *testing.T) {
	code := loadContract(t)
	for _, key := range []string{"components", "schemas", "Error", "properties", "code"} {
		next, ok := code[key].(map[string]any)
		if !ok {
			t.Fatalf("contract has no components.schemas.Error.properties.code (stopped at %q)", key)
		}
		code = next
	}
	enum, _ := code["enum"].([]any)
	if len(enum) == 0 {
		t.Fatalf("Error.code declares no enum in the contract; the catalogue is not the reference the rule promises")
	}

	got := make([]string, 0, len(enum))
	for _, v := range enum {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("Error.code enum holds a non-string value %v", v)
		}
		got = append(got, s)
	}
	slices.Sort(got)
	if want := httpCodes(); !slices.Equal(got, want) {
		t.Errorf("Error.code enum = %v\nwant the HTTP-surfaced catalogue %v", got, want)
	}
}

func httpCodes() []string {
	codes := errs.HTTPCodes()
	out := make([]string, 0, len(codes))
	for _, c := range codes {
		out = append(out, string(c))
	}
	return out
}
