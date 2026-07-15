// Package uuidx issues UUIDv7 identifiers for the gateway's domain objects.
//
// UUIDv7 (RFC 9562) embeds a Unix-millisecond timestamp in its high bits, so identifiers sort
// chronologically. That property is load-bearing here: it keeps PostgreSQL B-tree inserts
// append-only under load and matches the native uuidv7() generator the DDL uses for
// server-side defaults (migrations/0001_init.up.sql), so IDs minted in Go and in SQL share
// one ordering.
package uuidx

import (
	"fmt"

	"github.com/google/uuid"
)

// New returns a new UUIDv7. It panics only if the system entropy source fails, which is not a
// condition any caller can meaningfully handle — see NewE for the fallible form.
func New() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		panic(fmt.Sprintf("uuidx: generate v7: %v", err))
	}
	return id
}

// NewE returns a new UUIDv7, reporting an entropy failure instead of panicking. Prefer New
// unless the caller is on a path that must degrade rather than crash.
func NewE() (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generate uuidv7: %w", err)
	}
	return id, nil
}

// Parse decodes the canonical string form of a UUID.
func Parse(s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse uuid %q: %w", s, err)
	}
	return id, nil
}

// IsV7 reports whether id carries version 7. Use it to validate identifiers crossing a trust
// boundary when chronological ordering is assumed downstream.
func IsV7(id uuid.UUID) bool {
	return id.Version() == 7
}
