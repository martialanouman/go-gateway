// Package controlplane holds the domain types of the SMS gateway's control plane: the entities
// an operator provisions (customers, SMPP accounts, credentials, SMSC connectors, routes, sender
// IDs) and the enumerations that constrain them.
//
// It is a leaf package by design. It imports only the standard library, google/uuid, and
// internal/platform/errors — never a storage or transport package. Both the persistence layer
// (internal/storage/postgres) and the HTTP layer (internal/adminapi) depend on it, so a dependency
// the other way would be a cycle; keeping the arrows pointing inward is what makes that impossible.
//
// The enumeration string values are the contract: they match, character for character, the
// CHECK (... IN (...)) constraints of migrations/0001_init.up.sql and the enum lists of
// api/openapi-admin.yaml. A divergence is a bug, and enums_test.go proves the DDL side of it
// against the migration file itself.
//
// Known limitation — no null-clearing on the *Patch types (M1). A nil field in a *Patch means
// "leave unchanged": the postgres repositories translate it to COALESCE(narg, col), which cannot
// distinguish "absent" from "set to NULL". A PATCH that sends an explicit null for a nullable
// field (e.g. overdraft_limit, throughput_limit_per_sec, a route match_* pattern) is therefore a
// silent no-op, even though the OpenAPI contract marks those fields nullable. Reinstating a value
// to NULL is not possible through the Admin API at M1. Tri-state (absent vs null vs value) is
// deferred; it is tracked in docs/plan-execution-passerelle.md and must be revisited if a later
// milestone does not otherwise cover it.
package controlplane
