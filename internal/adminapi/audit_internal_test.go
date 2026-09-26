package adminapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
)

// TestAuditedReadsNameRegisteredOperations: the audit middleware recognises the reads it records by
// operation id; a renamed operation would silently drop out of the trail, so every id must be served.
func TestAuditedReadsNameRegisteredOperations(t *testing.T) {
	_, api := New(Deps{})
	served := map[string]bool{}
	for _, item := range api.OpenAPI().Paths {
		for _, op := range []*huma.Operation{item.Get, item.Post, item.Put, item.Patch, item.Delete} {
			if op != nil {
				served[op.OperationID] = true
			}
		}
	}
	for name, ids := range map[string]map[string]bool{"revealReads": revealReads, "unconditionalReads": unconditionalReads} {
		if len(ids) == 0 {
			t.Errorf("%s is empty: nothing guards those reads", name)
		}
		for id := range ids {
			if !served[id] {
				t.Errorf("%s names %q, which the Admin API does not serve", name, id)
			}
		}
	}
}

// TestReadOnlyPostSuffixesNameKnownDiagnostics: a POST whose path ends in /validate, /test,
// /test-connection or /check is classified as a read — it neither announces a config change nor enters the
// audit trail. That is safe only while those four operations really write nothing, and a scope declaration
// does not say so (test-billing-provider is admin:write). This pins the list: a new POST that lands on one
// of those suffixes fails here, and its author has to decide.
func TestReadOnlyPostSuffixesNameKnownDiagnostics(t *testing.T) {
	want := map[string]bool{
		"validate-routing-script":  true,
		"test-routing-script":      true,
		"test-billing-provider":    true,
		"check-suppression":        true,
		"test-sender-rewrite-rule": true,
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

	// The classification runs on the RESOLVED path, so a POST whose last segment is a path parameter could
	// be exonerated by its own data — an exact route named "check" would leave neither an audit row nor a
	// config-change event. No such route exists; this keeps it that way.
	for path, item := range api.OpenAPI().Paths {
		if item.Post != nil && strings.HasSuffix(path, "}") {
			t.Errorf("POST %s ends with a path parameter: its value could end in a read-only suffix and "+
				"exonerate a write from the audit trail", path)
		}
	}
}

// TestEveryMSISDNPathIsMaskedInTheTrail: a path that carries a subscriber number records it in the audit
// target. A new such operation missing from msisdnInTarget would serve the number in clear to audit:read.
func TestEveryMSISDNPathIsMaskedInTheTrail(t *testing.T) {
	_, api := New(Deps{})
	for path, item := range api.OpenAPI().Paths {
		for _, op := range []*huma.Operation{item.Get, item.Post, item.Put, item.Patch, item.Delete} {
			if op == nil || !strings.Contains(path, "{msisdn}") {
				continue
			}
			if !strings.Contains(path, exactRoutesPrefix) {
				t.Errorf("%s %s carries a number outside %s: toAuditEntryDTO would not find it to mask", op.Method, path, exactRoutesPrefix)
			}
			if op.Method != http.MethodGet && !msisdnInTarget[op.OperationID] {
				t.Errorf("%s %s records a number in its audit target, but msisdnInTarget does not mask it", op.Method, path)
			}
		}
	}
}
