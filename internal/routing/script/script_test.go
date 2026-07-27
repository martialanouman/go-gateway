package script_test

import (
	"testing"

	"github.com/martialanouman/go-gateway/internal/routing/script"
)

// TestChecksumIsDeterministic: the same source yields the same digest (so a compiled program can be
// cached by it) and any change yields a different one (so an edit is detected).
func TestChecksumIsDeterministic(t *testing.T) {
	src := "function resolveRoute(m){return null}"
	if a, b := script.Checksum(src), script.Checksum(src); a != b {
		t.Errorf("Checksum not stable: %q != %q", a, b)
	}
	if script.Checksum(src) == script.Checksum(src+" ") {
		t.Error("Checksum collided on different sources")
	}
	if len(script.Checksum(src)) != 64 {
		t.Errorf("Checksum length = %d, want 64 hex chars (sha256)", len(script.Checksum(src)))
	}
}
