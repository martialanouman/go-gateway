// Package adminapi is the Admin API: the HTTP surface an operator uses to provision the control
// plane (customers, SMPP accounts, credentials, connectors, routes, sender IDs).
//
// The contract is the source of truth, not this code: api/openapi-admin.yaml defines every
// operation, and contract_test.go fails the build if the spec Huma generates from these handlers
// drifts from it. Handlers implement the contract; they never redefine it.
//
// The persistence interfaces the handlers need are declared here, on the consumer side
// (guide-codage-go §6): internal/storage/postgres supplies concrete repositories that satisfy them
// structurally, and this package never imports that one. Errors crossing the store boundary carry
// an errs.Code, which humaerr maps to the flat wire model.
package adminapi
