package adminapi

import (
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
)

// TestRevealReadsNameRegisteredOperations: the audit middleware recognises the number-revealing reads by
// operation id; a renamed operation would silently drop out of the trail, so every id must be served.
func TestRevealReadsNameRegisteredOperations(t *testing.T) {
	_, api := New(Deps{})
	served := map[string]bool{}
	for _, item := range api.OpenAPI().Paths {
		for _, op := range []*huma.Operation{item.Get, item.Post, item.Put, item.Patch, item.Delete} {
			if op != nil {
				served[op.OperationID] = true
			}
		}
	}
	for id := range revealReads {
		if !served[id] {
			t.Errorf("revealReads names %q, which the Admin API does not serve", id)
		}
	}
	if len(revealReads) == 0 {
		t.Error("revealReads is empty: nothing guards the reveal reads")
	}
}

// TestReadOnlyPostSuffixesNameKnownDiagnostics: a POST whose path ends in /validate, /test,
// /test-connection or /check is classified as a read — it neither announces a config change nor enters the
// audit trail. That is safe only while those four operations really write nothing, and a scope declaration
// does not say so (test-billing-provider is admin:write). This pins the list: a new POST that lands on one
// of those suffixes fails here, and its author has to decide.
func TestReadOnlyPostSuffixesNameKnownDiagnostics(t *testing.T) {
	want := map[string]bool{
		"validate-routing-script": true,
		"test-routing-script":     true,
		"test-billing-provider":   true,
		"check-suppression":       true,
	}

	_, api := New(Deps{})
	for path, item := range api.OpenAPI().Paths {
		if item.Post == nil {
			continue
		}
		for _, suffix := range readOnlyPostSuffixes {
			if !strings.HasSuffix(path, suffix) {
				continue
			}
			if !want[item.Post.OperationID] {
				t.Errorf("POST %s (%s) is classified read-only by its suffix %q: it must write nothing, or the "+
					"suffix list must change", path, item.Post.OperationID, suffix)
			}
			delete(want, item.Post.OperationID)
		}
	}
	for id := range want {
		t.Errorf("%q no longer matches any read-only suffix: the classification silently changed", id)
	}
}
