// Package exact implements exact-number routing (schema §19): an MSISDN mapped to a specific connector
// or route — a ported number / MNP override that bypasses route resolution (the L0 short-cut).
//
// It owns the domain types, the in-memory Bloom filter, the L0 resolver and the cache invalidator. The
// durable table lives in internal/storage/postgres; the Redis key exactroute:{msisdn} is a read-through
// cache this package alone knows the wire form of — the resolver populates it, the Invalidator clears
// it on behalf of the Admin API (ADR-0015).
package exact

import (
	"time"

	"github.com/google/uuid"
)

// TargetType is what an exact route points at (control_plane.exact_routes.target_type).
type TargetType string

// The exact-route target kinds. target_id is polymorphic against these (no single FK).
const (
	TargetConnector TargetType = "connector"
	TargetRoute     TargetType = "route"
)

// Target is the destination an exact-number route resolves to.
type Target struct {
	Type TargetType
	ID   uuid.UUID
}

// Source records how an exact route was created (control_plane.exact_routes.source).
type Source string

// The exact-route provenance values.
const (
	SourceMNPImport   Source = "mnp_import"
	SourceManual      Source = "manual"
	SourceCarrierFeed Source = "carrier_feed"
)

// Route is one exact-number route: an E.164 MSISDN mapped to a Target, with its provenance. ImportedAt
// is set only for an imported route (nil for a manually-created one); UpdatedAt is maintained by the
// store.
type Route struct {
	MSISDN     string
	Target     Target
	Source     Source
	ImportedAt *time.Time
	UpdatedAt  time.Time
}
