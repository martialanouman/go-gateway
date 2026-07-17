package adminapi_test

import (
	"os"
	"sort"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

// The Bruno/OpenCollection test collection at api/collections/admin-api.yaml is a hand-maintained
// companion to the Admin API: one request per implemented operation, named after its operationId.
// This test is the guard that keeps it honest — it must track the registered surface exactly, so a
// newly landed endpoint that nobody added to the collection (or a request left behind after an
// endpoint is removed) fails the build instead of drifting silently. When it fails, update
// api/collections/admin-api.yaml to match: add the new operationId with a realistic body, or drop
// the stale request.
const collectionPath = "../../api/collections/admin-api.yaml"

// ocItem mirrors the OpenCollection item shape: an http request or a folder that nests more items.
type ocItem struct {
	Info struct {
		Name string `yaml:"name"`
		Type string `yaml:"type"`
	} `yaml:"info"`
	Items []ocItem `yaml:"items"`
}

// collectionRequestNames returns the set of http request names across the collection, at any depth.
func collectionRequestNames(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(collectionPath)
	if err != nil {
		t.Fatalf("read collection: %v", err)
	}
	var doc struct {
		Items []ocItem `yaml:"items"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse collection: %v", err)
	}
	names := map[string]bool{}
	var walk func(items []ocItem)
	walk = func(items []ocItem) {
		for _, it := range items {
			if it.Info.Type == "http" {
				names[it.Info.Name] = true
			}
			walk(it.Items)
		}
	}
	walk(doc.Items)
	return names
}

// registeredOperationIDs returns every operationId Huma registers for the Admin API.
func registeredOperationIDs(t *testing.T) map[string]bool {
	t.Helper()
	generated := loadGenerated(t)
	ids := map[string]bool{}
	paths, _ := generated["paths"].(map[string]any)
	for _, item := range paths {
		methods, _ := item.(map[string]any)
		for method, node := range methods {
			switch method {
			case "get", "post", "put", "patch", "delete", "head", "options", "trace":
				op, _ := node.(map[string]any)
				if id := str(op["operationId"]); id != "" {
					ids[id] = true
				}
			}
		}
	}
	return ids
}

// TestTestCollectionCoversExactlyTheImplementedSurface: the OpenCollection lists one request per
// registered operation — no more, no less. This is the "update the collection as we build" guard.
func TestTestCollectionCoversExactlyTheImplementedSurface(t *testing.T) {
	registered := registeredOperationIDs(t)
	inCollection := collectionRequestNames(t)

	for id := range registered {
		if !inCollection[id] {
			t.Errorf("operation %q is implemented but has no request in %s — add it", id, collectionPath)
		}
	}
	for name := range inCollection {
		if !registered[name] {
			t.Errorf("request %q in %s matches no implemented operation — remove it or fix its name",
				name, collectionPath)
		}
	}

	if t.Failed() {
		t.Logf("registered operations: %v", sortedKeys(registered))
		t.Logf("collection requests:   %v", sortedKeys(inCollection))
	}
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
