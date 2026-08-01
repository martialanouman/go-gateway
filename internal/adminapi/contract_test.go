package adminapi_test

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	yaml "go.yaml.in/yaml/v3"

	"github.com/martialanouman/go-gateway/internal/adminapi"
)

// opRef identifies an operation by its method and path.
type opRef struct {
	id     string
	method string
	path   string
}

// m1Operations is the M1 Admin-API surface. It grows as each resource lands and is complete at the
// end of the milestone. An operation absent here is one nothing asserts, so the list is kept honest
// by TestGeneratedSpecRegistersNoOperationOutsideTheM1Surface once every resource is in.
var m1Operations = []opRef{
	{"list-customers", "get", "/admin/customers"},
	{"create-customer", "post", "/admin/customers"},
	{"get-customer", "get", "/admin/customers/{id}"},
	{"update-customer", "patch", "/admin/customers/{id}"},
	{"delete-customer", "delete", "/admin/customers/{id}"},
	{"suspend-customer", "post", "/admin/customers/{id}/suspend"},

	{"list-smpp-accounts", "get", "/admin/smpp-accounts"},
	{"create-smpp-account", "post", "/admin/smpp-accounts"},
	{"get-smpp-account", "get", "/admin/smpp-accounts/{id}"},
	{"update-smpp-account", "patch", "/admin/smpp-accounts/{id}"},
	{"delete-smpp-account", "delete", "/admin/smpp-accounts/{id}"},
	{"set-account-channels", "patch", "/admin/smpp-accounts/{id}/channels"},
	{"set-account-session-limits", "patch", "/admin/smpp-accounts/{id}/session-limits"},

	{"list-credentials", "get", "/admin/smpp-accounts/{id}/credentials"},
	{"create-credential", "post", "/admin/smpp-accounts/{id}/credentials"},
	{"update-credential-status", "patch", "/admin/smpp-accounts/{id}/credentials/{credId}"},
	{"revoke-credential", "delete", "/admin/smpp-accounts/{id}/credentials/{credId}"},
	{"rotate-credential", "post", "/admin/smpp-accounts/{id}/credentials/{credId}/rotate"},

	{"list-connectors", "get", "/admin/connectors"},
	{"create-connector", "post", "/admin/connectors"},
	{"get-connector", "get", "/admin/connectors/{id}"},
	{"update-connector", "patch", "/admin/connectors/{id}"},
	{"delete-connector", "delete", "/admin/connectors/{id}"},
	{"rebind-connector", "post", "/admin/connectors/{id}/rebind"},
	{"get-connector-status", "get", "/admin/connectors/{id}/status"},
	{"set-connector-reconnect-policy", "patch", "/admin/connectors/{id}/reconnect-policy"},
	{"set-connector-bind-pool", "patch", "/admin/connectors/{id}/bind-pool"},

	{"list-routes", "get", "/admin/routes"},
	{"create-route", "post", "/admin/routes"},
	{"get-route", "get", "/admin/routes/{id}"},
	{"update-route", "patch", "/admin/routes/{id}"},
	{"delete-route", "delete", "/admin/routes/{id}"},

	{"list-sender-ids", "get", "/admin/customers/{id}/sender-ids"},
	{"create-sender-id", "post", "/admin/customers/{id}/sender-ids"},
	{"update-sender-id", "patch", "/admin/customers/{id}/sender-ids/{senderId}"},
	{"delete-sender-id", "delete", "/admin/customers/{id}/sender-ids/{senderId}"},

	{"list-inbound-numbers", "get", "/admin/inbound-numbers"},
	{"create-inbound-number", "post", "/admin/inbound-numbers"},
	{"update-inbound-number", "patch", "/admin/inbound-numbers/{id}"},
	{"delete-inbound-number", "delete", "/admin/inbound-numbers/{id}"},
	{"assign-inbound-number", "patch", "/admin/inbound-numbers/{id}/assign"},

	{"list-inbound-keywords", "get", "/admin/inbound-numbers/{id}/keywords"},
	{"create-inbound-keyword", "post", "/admin/inbound-numbers/{id}/keywords"},
	{"update-inbound-keyword", "patch", "/admin/inbound-numbers/{id}/keywords/{keywordId}"},
	{"delete-inbound-keyword", "delete", "/admin/inbound-numbers/{id}/keywords/{keywordId}"},

	{"list-unrouted-mo", "get", "/admin/mo/unrouted"},

	{"list-suppressions", "get", "/admin/suppressions"},
	{"create-suppression", "post", "/admin/suppressions"},
	{"import-suppressions", "post", "/admin/suppressions/import"},
	{"check-suppression", "post", "/admin/suppressions/check"},
	{"delete-suppression", "delete", "/admin/suppressions/{id}"},

	{"list-opt-out-keywords", "get", "/admin/opt-out-keywords"},
	{"create-opt-out-keyword", "post", "/admin/opt-out-keywords"},
	{"update-opt-out-keyword", "patch", "/admin/opt-out-keywords/{id}"},
	{"delete-opt-out-keyword", "delete", "/admin/opt-out-keywords/{id}"},

	{"list-antispam-rules", "get", "/admin/antispam-rules"},
	{"create-antispam-rule", "post", "/admin/antispam-rules"},
	{"update-antispam-rule", "patch", "/admin/antispam-rules/{id}"},
	{"delete-antispam-rule", "delete", "/admin/antispam-rules/{id}"},

	{"list-exact-routes", "get", "/admin/exact-routes"},
	{"create-exact-route", "post", "/admin/exact-routes"},
	{"import-exact-routes", "post", "/admin/exact-routes/import"},
	{"lookup-exact-route", "get", "/admin/exact-routes/lookup"},
	{"update-exact-route", "patch", "/admin/exact-routes/{msisdn}"},
	{"delete-exact-route", "delete", "/admin/exact-routes/{msisdn}"},

	{"list-routing-scripts", "get", "/admin/routing-scripts"},
	{"create-routing-script", "post", "/admin/routing-scripts"},
	{"get-routing-script", "get", "/admin/routing-scripts/{id}"},
	{"update-routing-script", "patch", "/admin/routing-scripts/{id}"},
	{"delete-routing-script", "delete", "/admin/routing-scripts/{id}"},
	{"list-routing-script-versions", "get", "/admin/routing-scripts/{id}/versions"},
	{"assign-routing-script", "patch", "/admin/routing-scripts/{id}/assign"},
	{"validate-routing-script", "post", "/admin/routing-scripts/{id}/validate"},
	{"test-routing-script", "post", "/admin/routing-scripts/{id}/test"},
	{"publish-routing-script", "post", "/admin/routing-scripts/{id}/publish"},

	{"get-customer-billing", "get", "/admin/customers/{id}/billing"},
	{"update-customer-billing", "patch", "/admin/customers/{id}/billing"},
	{"get-customer-balances", "get", "/admin/customers/{id}/balances"},
	{"topup-balance", "post", "/admin/customers/{id}/billing/topup"},
	{"transfer-balance", "post", "/admin/customers/{id}/billing/transfer"},
	{"change-balance-scope", "post", "/admin/customers/{id}/billing/scope"},

	{"get-billing-ledger", "get", "/admin/customers/{id}/billing/ledger"},
	{"list-rate-plans", "get", "/admin/rate-plans"},
	{"create-rate-plan", "post", "/admin/rate-plans"},
	{"update-rate-plan", "patch", "/admin/rate-plans/{id}"},
	{"delete-rate-plan", "delete", "/admin/rate-plans/{id}"},
	{"list-billing-providers", "get", "/admin/billing-providers"},
	{"create-billing-provider", "post", "/admin/billing-providers"},
	{"update-billing-provider", "patch", "/admin/billing-providers/{id}"},
	{"delete-billing-provider", "delete", "/admin/billing-providers/{id}"},
	{"test-billing-provider", "post", "/admin/billing-providers/{id}/test-connection"},

	{"rotate-content-key", "post", "/admin/customers/{id}/content/rotate-key"},
	{"get-message-content", "get", "/admin/messages/{id}/content"},
	{"erase-customer-content", "post", "/admin/customers/{id}/content/erase"},
	{"gdpr-erase", "post", "/admin/gdpr/erase"},
	{"get-gdpr-erase-job", "get", "/admin/gdpr/erase/{jobId}"},
}

// loadContract reads api/openapi-admin.yaml (the source of truth) into a generic tree.
func loadContract(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("../../api/openapi-admin.yaml")
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse contract: %v", err)
	}
	return normalizeYAMLMaps(doc)
}

// loadGenerated builds the Admin API with nil stores (registration touches none) and returns the
// OpenAPI Huma generated, as a generic tree.
func loadGenerated(t *testing.T) map[string]any {
	t.Helper()
	_, api := adminapi.New(adminapi.Deps{})
	raw, err := api.OpenAPI().MarshalJSON()
	if err != nil {
		t.Fatalf("marshal generated spec: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse generated spec: %v", err)
	}
	return doc
}

// TestContractCoversEveryM1Operation: every operation M1 claims to implement exists in both the
// contract and the generated spec, at the stated method and path.
func TestContractCoversEveryM1Operation(t *testing.T) {
	contract := loadContract(t)
	generated := loadGenerated(t)

	for _, op := range m1Operations {
		if operationNode(contract, op.path, op.method) == nil {
			t.Errorf("contract is missing %s %s (%s)", op.method, op.path, op.id)
		}
		if operationNode(generated, op.path, op.method) == nil {
			t.Errorf("generated spec is missing %s %s (%s)", op.method, op.path, op.id)
		}
	}
}

// TestGeneratedSpecMatchesTheContractForEveryM1Operation is the strict comparison: for each M1
// operation, the operationId, the exact set of response status codes, and the resolved request and
// response schemas must match the contract, field by field. Description, summary, example, ordering,
// and the keywords the contract is internally inconsistent about are normalized away.
func TestGeneratedSpecMatchesTheContractForEveryM1Operation(t *testing.T) {
	contract := loadContract(t)
	generated := loadGenerated(t)

	for _, op := range m1Operations {
		t.Run(op.id, func(t *testing.T) {
			cOp := operationNode(contract, op.path, op.method)
			gOp := operationNode(generated, op.path, op.method)
			if cOp == nil || gOp == nil {
				t.Fatalf("operation missing (contract=%v generated=%v)", cOp != nil, gOp != nil)
			}

			if got := str(gOp["operationId"]); got != op.id {
				t.Errorf("operationId = %q, want %q", got, op.id)
			}

			cCodes := responseCodes(cOp)
			gCodes := responseCodes(gOp)
			if !reflect.DeepEqual(cCodes, gCodes) {
				t.Errorf("response codes differ:\n contract:  %v\n generated: %v", cCodes, gCodes)
			}

			compareSchemas(t, "requestBody", requestSchema(contract, cOp), requestSchema(generated, gOp))
			for _, code := range cCodes {
				compareSchemas(t, "response "+code,
					responseSchema(contract, cOp, code), responseSchema(generated, gOp, code))
			}
		})
	}
}

// TestGeneratedSpecRegistersNoOperationOutsideTheM1Surface is the direction people forget: the
// service must expose ONLY the M1 operations, nothing more. It is what catches a copy-pasted handler
// or an operation that slipped in from a later milestone. It is enabled at the close of M1, once
// m1Operations is complete.
func TestGeneratedSpecRegistersNoOperationOutsideTheM1Surface(t *testing.T) {
	generated := loadGenerated(t)

	want := map[string]bool{}
	for _, op := range m1Operations {
		want[op.method+" "+op.path] = true
	}

	paths, _ := generated["paths"].(map[string]any)
	for path, item := range paths {
		methods, _ := item.(map[string]any)
		for method := range methods {
			switch method {
			case "get", "post", "put", "patch", "delete", "head", "options", "trace":
				if !want[method+" "+path] {
					op, _ := methods[method].(map[string]any)
					t.Errorf("generated spec exposes %s %s (%s), which is not in the M1 surface",
						method, path, str(op["operationId"]))
				}
			}
		}
	}
}

// TestErrorSchemaIsTheFlatContractModel: the error body both sides serve is the flat
// {code, message, errors[]}, with code and message required — not an RFC 9457 problem document.
func TestErrorSchemaIsTheFlatContractModel(t *testing.T) {
	generated := loadGenerated(t)
	// The 404 response of get-customer carries the error model on the generated side.
	op := operationNode(generated, "/admin/customers/{id}", "get")
	schema := responseSchema(generated, op, "404")

	props, _ := schema["properties"].(map[string]any)
	if props == nil {
		t.Fatalf("error schema has no properties: %v", schema)
	}
	for _, want := range []string{"code", "message"} {
		if _, ok := props[want]; !ok {
			t.Errorf("error schema is missing required property %q", want)
		}
	}
	required := toStringSet(schema["required"])
	if !required["code"] || !required["message"] {
		t.Errorf("error schema required = %v, want code and message", schema["required"])
	}
	for _, forbidden := range []string{"type", "title", "detail", "status", "instance"} {
		if _, ok := props[forbidden]; ok {
			t.Errorf("error schema carries RFC 9457 field %q; the model must be flat", forbidden)
		}
	}
}

// --- navigation helpers ---

func operationNode(doc map[string]any, path, method string) map[string]any {
	paths, _ := doc["paths"].(map[string]any)
	item, _ := paths[path].(map[string]any)
	op, _ := item[method].(map[string]any)
	return op
}

func responseCodes(op map[string]any) []string {
	resp, _ := op["responses"].(map[string]any)
	codes := make([]string, 0, len(resp))
	for code := range resp {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

// requestSchema returns the fully resolved, normalized request-body schema, or nil when the
// operation has no body.
func requestSchema(doc, op map[string]any) map[string]any {
	rb, _ := op["requestBody"].(map[string]any)
	if rb == nil {
		return nil
	}
	schema := jsonSchemaOf(rb)
	if schema == nil {
		return nil
	}
	return resolve(doc, schema, map[string]bool{})
}

// responseSchema returns the fully resolved, normalized schema of a response code, or nil when it
// has no body.
func responseSchema(doc, op map[string]any, code string) map[string]any {
	resp, _ := op["responses"].(map[string]any)
	r, _ := resp[code].(map[string]any)
	if r == nil {
		return nil
	}
	// A response may itself be a $ref to components/responses.
	r = deref(doc, r, map[string]bool{})
	schema := jsonSchemaOf(r)
	if schema == nil {
		return nil
	}
	return resolve(doc, schema, map[string]bool{})
}

// jsonSchemaOf digs schema out of content["application/json"].schema.
func jsonSchemaOf(node map[string]any) map[string]any {
	content, _ := node["content"].(map[string]any)
	appjson, _ := content["application/json"].(map[string]any)
	schema, _ := appjson["schema"].(map[string]any)
	return schema
}

func compareSchemas(t *testing.T, label string, want, got map[string]any) {
	t.Helper()
	if (want == nil) != (got == nil) {
		t.Errorf("%s: one side has a schema and the other does not (contract=%v generated=%v)",
			label, want != nil, got != nil)
		return
	}
	if want == nil {
		return
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("%s schema drift:\n contract:  %s\n generated: %s", label, dump(want), dump(got))
	}
}

func dump(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func toStringSet(v any) map[string]bool {
	out := map[string]bool{}
	list, _ := v.([]any)
	for _, e := range list {
		if s, ok := e.(string); ok {
			out[s] = true
		}
	}
	return out
}

// --- normalization ---

// deref replaces a {"$ref": "#/components/..."} node with the node it points at, recursively. The
// seen set breaks the cycles the self-referential Route schema (fallback_route_id) would otherwise
// cause: a re-entry is substituted with a sentinel that compares equal to itself.
func deref(doc, node map[string]any, seen map[string]bool) map[string]any {
	ref, ok := node["$ref"].(string)
	if !ok {
		return node
	}
	if seen[ref] {
		return map[string]any{"$circular": ref}
	}
	target := resolveRef(doc, ref)
	if target == nil {
		return node
	}
	next := map[string]bool{}
	for k := range seen {
		next[k] = true
	}
	next[ref] = true
	return deref(doc, target, next)
}

func resolveRef(doc map[string]any, ref string) map[string]any {
	// ref looks like #/components/schemas/Customer
	if len(ref) < 2 || ref[0] != '#' {
		return nil
	}
	cur := any(doc)
	for _, part := range splitRef(ref[2:]) {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	m, _ := cur.(map[string]any)
	return m
}

func splitRef(p string) []string {
	var parts []string
	cur := ""
	for _, r := range p {
		if r == '/' {
			parts = append(parts, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		parts = append(parts, cur)
	}
	return parts
}

// mergeAllOf merges an allOf list into one object schema: union of properties, union of required.
// Huma flattens embedded structs into a single object, while the contract expresses composites such
// as CustomerPage as allOf[$ref PageMeta, {data}]; without this, every paginated response reports a
// false drift. Each member is resolved first (following its $ref) so a referenced schema such as
// PageMeta contributes its actual properties rather than a bare $ref.
func mergeAllOf(doc, node map[string]any, seen map[string]bool) map[string]any {
	list, ok := node["allOf"].([]any)
	if !ok {
		return node
	}
	merged := map[string]any{"type": "object"}
	props := map[string]any{}
	var required []any
	for _, m := range list {
		sub, _ := m.(map[string]any)
		sub = resolve(doc, sub, seen)
		for k, v := range subProps(sub) {
			props[k] = v
		}
		required = append(required, subRequired(sub)...)
	}
	// Merge any properties declared alongside the allOf itself.
	for k, v := range subProps(node) {
		props[k] = v
	}
	required = append(required, subRequired(node)...)

	if len(props) > 0 {
		merged["properties"] = props
	}
	if len(required) > 0 {
		merged["required"] = required
	}
	return merged
}

func subProps(node map[string]any) map[string]any {
	p, _ := node["properties"].(map[string]any)
	return p
}

func subRequired(node map[string]any) []any {
	r, _ := node["required"].([]any)
	return r
}

// resolve is the whole normalization: it dereferences the node (following nested $refs with the
// cycle guard), flattens allOf, drops documentation and the keywords the contract is internally
// inconsistent about, canonicalizes nullability, and recurses into every child. Running deref at
// every level is what makes a contract $ref to Customer and a generated $ref to CustomerDTO — or an
// enum $ref and its inline twin — compare equal by their resolved content.
func resolve(doc, node map[string]any, seen map[string]bool) map[string]any {
	if node == nil {
		return nil
	}
	node = deref(doc, node, seen)
	node = mergeAllOf(doc, node, seen)

	out := map[string]any{}
	for k, v := range node {
		switch k {
		case "description", "summary", "example", "examples", "title", "tags",
			"deprecated", "externalDocs", "readOnly", "writeOnly", "default",
			"additionalProperties", "$schema", "xml", "discriminator", "nullable":
			continue
		default:
			out[k] = resolveValue(doc, v, seen)
		}
	}

	if isNullable(node) {
		out["type"] = withNull(out["type"])
	}
	if t, ok := out["type"]; ok {
		out["type"] = dropArrayNull(canonType(t))
	}
	if enum, ok := out["enum"].([]any); ok {
		out["enum"] = canonEnum(enum, out["type"])
	}
	// Huma tags numeric Go types with a format the contract does not carry (int64/int32 for ints,
	// double/float for floats). Drop those; keep meaningful formats like uuid and date-time.
	if f, ok := out["format"].(string); ok {
		switch f {
		case "int64", "int32", "double", "float":
			delete(out, "format")
		}
	}
	if props, ok := out["properties"].(map[string]any); ok {
		delete(props, "$schema")
		if len(props) == 0 {
			delete(out, "properties")
		}
	}
	if req, ok := out["required"].([]any); ok {
		out["required"] = sortAny(req)
	}
	return out
}

func resolveValue(doc map[string]any, v any, seen map[string]bool) any {
	switch t := v.(type) {
	case map[string]any:
		return resolve(doc, t, seen)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = resolveValue(doc, e, seen)
		}
		return out
	// Numbers must be coerced to one type: YAML decodes an integer as int, JSON as float64, so a
	// bare `minimum: 0` would otherwise differ by type alone.
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	default:
		return v
	}
}

// dropArrayNull removes a "null" member from an array-typed union. Huma marks every Go slice
// nullable ([array,null]); the contract types its arrays as plain [array]. Whether a JSON array is
// null, absent, or empty is not a contract-meaningful distinction here, so the two are treated as
// equal by dropping the null on both sides.
func dropArrayNull(t any) any {
	list, ok := t.([]any)
	if !ok {
		return t
	}
	hasArray := false
	for _, e := range list {
		if e == "array" {
			hasArray = true
		}
	}
	if !hasArray {
		return t
	}
	out := make([]any, 0, len(list))
	for _, e := range list {
		if e == "null" {
			continue
		}
		out = append(out, e)
	}
	return out
}

func isNullable(node map[string]any) bool {
	b, _ := node["nullable"].(bool)
	return b
}

// withNull adds "null" to a type, turning a scalar into a union.
func withNull(t any) any {
	switch v := t.(type) {
	case string:
		return []any{v, "null"}
	case []any:
		for _, e := range v {
			if e == "null" {
				return v
			}
		}
		return append(v, "null")
	default:
		return t
	}
}

// canonType renders a type as a sorted []any so that "string" and ["string","null"] compare
// structurally and order never matters.
func canonType(t any) any {
	switch v := t.(type) {
	case string:
		return []any{v}
	case []any:
		strs := make([]string, 0, len(v))
		for _, e := range v {
			strs = append(strs, fmt.Sprint(e))
		}
		sort.Strings(strs)
		out := make([]any, len(strs))
		for i, s := range strs {
			out[i] = s
		}
		return out
	default:
		return t
	}
}

// canonEnum drops a null member when the type already allows null (the contract lists null in some
// enums; Huma expresses it through the type), and sorts the rest.
func canonEnum(enum []any, typ any) []any {
	typeHasNull := false
	if list, ok := typ.([]any); ok {
		for _, e := range list {
			if e == "null" {
				typeHasNull = true
			}
		}
	}
	var kept []string
	nullSeen := false
	for _, e := range enum {
		if e == nil {
			nullSeen = true
			continue
		}
		kept = append(kept, fmt.Sprint(e))
	}
	sort.Strings(kept)
	out := make([]any, 0, len(kept)+1)
	for _, s := range kept {
		out = append(out, s)
	}
	if nullSeen && !typeHasNull {
		out = append(out, nil)
	}
	return out
}

func sortAny(list []any) []any {
	strs := make([]string, 0, len(list))
	for _, e := range list {
		strs = append(strs, fmt.Sprint(e))
	}
	sort.Strings(strs)
	out := make([]any, len(strs))
	for i, s := range strs {
		out[i] = s
	}
	return out
}

// normalizeYAMLMaps converts the map[interface{}]interface{} that some YAML decoders emit into
// map[string]any. go.yaml.in/yaml/v3 already uses map[string]any, so this is a defensive pass that
// also recurses through slices.
func normalizeYAMLMaps(v any) map[string]any {
	m, _ := deepStringMap(v).(map[string]any)
	return m
}

func deepStringMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range t {
			out[k] = deepStringMap(val)
		}
		return out
	case map[any]any:
		out := map[string]any{}
		for k, val := range t {
			out[fmt.Sprint(k)] = deepStringMap(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = deepStringMap(e)
		}
		return out
	default:
		return v
	}
}
