// Package humaspec holds the huma spec helpers shared by the public and admin APIs: registering an
// operation while remembering the error codes it is meant to expose, and pruning the boilerplate
// responses huma injects so the served spec matches the hand-written contract exactly.
//
// Each API passes its own metadata key (the public and admin contracts declare different error sets
// and the strict contract tests compare the exact code set per API), so the key is a parameter
// rather than a constant. The logic itself is identical for both, which is why it lives here.
package humaspec

import (
	"context"
	"strconv"

	"github.com/danielgtaylor/huma/v2"
)

// Register wires an operation and records, under metaKey, the error codes it is meant to expose.
// huma appends its own 422/500 to Operation.Errors during registration, so the intended set must be
// captured separately here for Prune to consult later.
func Register[I, O any](api huma.API, metaKey string, op huma.Operation, handler func(context.Context, *I) (*O, error)) {
	if op.Metadata == nil {
		op.Metadata = map[string]any{}
	}
	op.Metadata[metaKey] = append([]int(nil), op.Errors...)
	huma.Register(api, op, handler)
}

// Prune narrows each operation's documented responses to exactly the set its contract declares: the
// success status plus the error codes recorded at registration under metaKey. huma otherwise injects
// a 500, a default, and a 422 the contract may not enumerate, and the strict contract test compares
// the exact set of codes. It documents nothing away that matters: a handler can still return any of
// those statuses; they are simply not enumerated, exactly as in the contract.
func Prune(api huma.API, metaKey string) {
	for _, item := range api.OpenAPI().Paths {
		for _, op := range operationsOf(item) {
			if op == nil {
				continue
			}
			allowed := declaredCodes(op, metaKey)
			for code := range op.Responses {
				if !allowed[code] {
					delete(op.Responses, code)
				}
			}
		}
	}
}

// declaredCodes is the set of response codes an operation is meant to expose: its success status
// (2xx/3xx already present) plus the codes recorded at registration. Reading op.Errors here would
// not work — huma appends its own 422 and 500 to that slice during registration, so the intended set
// is captured separately in metadata by Register.
func declaredCodes(op *huma.Operation, metaKey string) map[string]bool {
	allowed := map[string]bool{}
	for code := range op.Responses {
		if len(code) == 3 && (code[0] == '2' || code[0] == '3') {
			allowed[code] = true
		}
	}
	if codes, ok := op.Metadata[metaKey].([]int); ok {
		for _, code := range codes {
			allowed[strconv.Itoa(code)] = true
		}
	}
	return allowed
}

// operationsOf returns the operations present on a path item.
func operationsOf(item *huma.PathItem) []*huma.Operation {
	return []*huma.Operation{
		item.Get, item.Post, item.Put, item.Patch, item.Delete,
		item.Head, item.Options, item.Trace,
	}
}
