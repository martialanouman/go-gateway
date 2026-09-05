package restapi_test

import (
	"slices"
	"testing"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestErrorCodeEnumMatchesTheCatalogue (step-260h) is the public-contract twin of the Admin test: the
// Error.code enum equals the catalogue's HTTP-surfaced codes, in both directions.
func TestErrorCodeEnumMatchesTheCatalogue(t *testing.T) {
	got := slices.Clone(loadContract(t).Components.Schemas["Error"].Properties["code"].Enum)
	if len(got) == 0 {
		t.Fatalf("Error.code declares no enum in the public contract")
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
