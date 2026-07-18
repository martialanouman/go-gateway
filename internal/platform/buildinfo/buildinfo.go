// Package buildinfo exposes the build's version metadata. The values default to development
// placeholders and are overwritten at release time by the linker: goreleaser stamps them with
// -ldflags -X (see .goreleaser.yaml). Keeping them in one package means every binary reports the
// same version without each main having to declare its own vars.
package buildinfo

var (
	// Version is the SemVer release tag (e.g. "v1.4.0"), or "dev" for an unstamped local build.
	Version = "dev"
	// Commit is the git commit the build was cut from, or "none" when unstamped.
	Commit = "none"
	// Date is the RFC 3339 build timestamp, or "unknown" when unstamped.
	Date = "unknown"
)
