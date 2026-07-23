package controlplane

// RateLimit is the optional throughput ceiling configured for an entity (control_plane.rate_limits).
// Each field is a pointer because the column is individually nullable: nil means "no limit set for
// this dimension". A RateLimit only exists when a row is configured for the entity; the absence of a
// row is signalled by the caller, not by a zero value here.
type RateLimit struct {
	MaxPerSec     *int
	MaxPerDay     *int
	BurstCapacity *int
}
