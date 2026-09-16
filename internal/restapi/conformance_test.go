package restapi_test

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	yaml "go.yaml.in/yaml/v3"

	"github.com/martialanouman/go-gateway/internal/restapi"
)

// contractDoc is a minimal projection of api/openapi-public.yaml: enough to assert operation-level
// conformance without wrestling with huma's JSON-Schema serialization quirks.
type contractDoc struct {
	Security []map[string][]string `yaml:"security"`
	// Paths is typed to the verb map, which is also what keeps webhooks: (the outgoing on-mo/on-dlr
	// callbacks, OpenAPI 3.1) out of reach: they are not endpoints anyone serves, and a generic
	// map[string]any sweep would demand they be classified. The known limit, observed rather than
	// predicted: the day a path-item carries a `parameters:` key, this decode fails with
	// `parse contract: yaml: unmarshal errors: line N: cannot unmarshal !!seq into contractOp` and
	// t.Fatalf stops the test. That is loud, immediate and names the line — not a silent hole — so it
	// is left alone; the fix, when it comes, is to key this on the HTTP verbs. A non-verb key holding
	// a MAP (an x- extension) decodes fine instead, with an empty OperationID, and the coverage
	// assertion below then reports it as unclassified. Same fix, and equally loud.
	Paths      map[string]map[string]contractOp `yaml:"paths"`
	Components struct {
		Schemas map[string]struct {
			Properties map[string]struct {
				Enum []string `yaml:"enum"`
			} `yaml:"properties"`
		} `yaml:"schemas"`
	} `yaml:"components"`
}

type contractOp struct {
	OperationID string                `yaml:"operationId"`
	Responses   map[string]any        `yaml:"responses"`
	Security    []map[string][]string `yaml:"security"`
}

// deferredOp annotates an operation the contract declares and nobody serves yet. It is the same form
// internal/adminapi uses, declared here rather than shared: a package exported for one type of two
// fields would be an abstraction bought for nobody. The fields are asserted non-empty so the FIRST
// entry to land here arrives annotated, instead of starting the free-text list this form exists to
// prevent.
type deferredOp struct{ reason, step string }

// implemented are the operations the public API serves. deferred lists operations the contract
// declares but does not serve yet — none currently: cancel-message was removed from the contract
// because cancellation is SMPP-only (ADR-0009). The conformance test asserts the served spec is
// exactly the implemented set: it matches the contract for what it serves, serves nothing it should
// not, and — since step-320 — leaves nothing in the contract unclassified.
var (
	implemented = map[string]bool{"submit-messages": true, "get-message": true, "get-account": true, "list-messages": true, "health": true}
	deferred    = map[string]deferredOp{}
)

func loadContract(t *testing.T) contractDoc {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "api", "openapi-public.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	var doc contractDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse contract: %v", err)
	}
	return doc
}

type servedOp struct {
	path     string
	method   string
	codes    []string
	security []map[string][]string
}

// TestServedSpecConformsToContract checks that every operation M2 serves matches the contract on
// path, method, response status codes and security requirement, and that no deferred operation is
// served. Deep request/response schema equality is intentionally out of scope for M2 (the single-
// vs-batch submit body is a documented divergence); this guards the operation surface.
func TestServedSpecConformsToContract(t *testing.T) {
	contract := loadContract(t)
	// Without this, the whole test is a no-op the day the projection stops matching the YAML: every
	// assertion below hangs off ranging contract.Paths, so an empty decode passes in silence. Proven
	// by mutation — renaming the `yaml:"paths"` tag left this test green. adminapi is guarded by its
	// stale-entry property instead, but only while its deferred list is non-empty.
	if len(contract.Paths) == 0 {
		t.Fatal("contract declares no path: the projection no longer matches api/openapi-public.yaml")
	}

	// Build the API spec-only (nil Principals => no middleware, no requests issued).
	_, api := restapi.New(restapi.Deps{})
	served := api.OpenAPI()

	servedOps := map[string]servedOp{}
	for path, item := range served.Paths {
		for method, op := range operationsByMethod(item) {
			if op == nil {
				continue
			}
			servedOps[op.OperationID] = servedOp{
				path:     path,
				method:   method,
				codes:    responseCodes(op.Responses),
				security: op.Security,
			}
		}
	}

	for path, methods := range contract.Paths {
		for method, cop := range methods {
			// The direction the loop below cannot take: declared in the published contract, classified
			// by nobody. Without it, an operation added to the YAML and to neither map is skipped in
			// silence. There is no gap to absorb today (5 served of 5), which is exactly why it is
			// free to add now and will not be later.
			if _, isDeferred := deferred[cop.OperationID]; !implemented[cop.OperationID] && !isDeferred {
				t.Errorf("contract declares %s %s (%s): it is in neither implemented nor deferred",
					method, path, cop.OperationID)
			}
			if !implemented[cop.OperationID] {
				continue
			}
			sop, ok := servedOps[cop.OperationID]
			if !ok {
				t.Errorf("operation %q is not served", cop.OperationID)
				continue
			}
			if sop.path != path || sop.method != method {
				t.Errorf("%s: served %s %s, contract %s %s", cop.OperationID, sop.method, sop.path, method, path)
			}
			// M2 serves a documented subset of the contract (single-submit only, no health-degraded),
			// so served codes must be a subset of the contract's — nothing undocumented is served.
			if want := codeSet(cop.Responses); !subset(sop.codes, want) {
				t.Errorf("%s serves codes %v not all declared in the contract %v", cop.OperationID, sop.codes, want)
			}
			// The contract secures submit/get via the GLOBAL security (no per-op block); health
			// overrides to public with `security: []`.
			wantSecured := contractSecured(cop, secured(contract.Security))
			if wantSecured != secured(sop.security) {
				t.Errorf("%s security mismatch: served secured=%v contract secured=%v",
					cop.OperationID, secured(sop.security), wantSecured)
			}
		}
	}

	for id, entry := range deferred {
		if _, ok := servedOps[id]; ok {
			t.Errorf("operation %q is deferred but is served: drop the deferred entry", id)
		}
		if entry.reason == "" || entry.step == "" {
			t.Errorf("deferred %q has reason=%q step=%q: both are required", id, entry.reason, entry.step)
		}
	}
}

func operationsByMethod(item *huma.PathItem) map[string]*huma.Operation {
	return map[string]*huma.Operation{
		"get":    item.Get,
		"post":   item.Post,
		"put":    item.Put,
		"patch":  item.Patch,
		"delete": item.Delete,
	}
}

func responseCodes(responses map[string]*huma.Response) []string {
	out := make([]string, 0, len(responses))
	for code := range responses {
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

func codeSet(responses map[string]any) []string {
	out := make([]string, 0, len(responses))
	for code := range responses {
		out = append(out, code)
	}
	sort.Strings(out)
	return out
}

// secured reports whether a security block requires a scheme (a non-empty requirement).
func secured(requirements []map[string][]string) bool {
	for _, req := range requirements {
		if len(req) > 0 {
			return true
		}
	}
	return false
}

// contractSecured resolves an operation's effective security: an operation with no explicit block
// inherits the document's global security; an explicit (possibly empty) block overrides it.
func contractSecured(op contractOp, globalSecured bool) bool {
	if op.Security == nil {
		return globalSecured
	}
	return secured(op.Security)
}

// subset reports whether every element of a is in b.
func subset(a, b []string) bool {
	set := make(map[string]bool, len(b))
	for _, s := range b {
		set[s] = true
	}
	for _, s := range a {
		if !set[s] {
			return false
		}
	}
	return true
}
