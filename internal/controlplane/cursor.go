package controlplane

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// cursorVersion tags the encoding so a future scheme change is detectable rather than silently
// mis-decoded. It is part of the opaque payload, never surfaced to a client.
const cursorVersion = "cp1"

// Cursor is an opaque page position for keyset pagination. It is opaque on purpose: a client that
// learns it wraps a UUID would start constructing its own, and the pagination scheme could then
// never change. The listings are keyset-paginated on the UUIDv7 primary key, which is time-ordered
// by construction (RFC 9562), so a position is a single id and needs no secondary sort key.
type Cursor string

// EncodeCursor renders the page position after id. It returns the empty cursor for the nil UUID,
// which is what a repository emits when there is no next page.
func EncodeCursor(id uuid.UUID) Cursor {
	if id == uuid.Nil {
		return ""
	}
	payload := cursorVersion + ":" + id.String()
	return Cursor(base64.RawURLEncoding.EncodeToString([]byte(payload)))
}

// DecodeCursor parses c into the id it encodes. It returns uuid.Nil for the empty cursor (the first
// page). A malformed cursor is an errs.ErrValidation, never an internal error: it arrives from a
// client and a 422 is the honest answer.
func DecodeCursor(c Cursor) (uuid.UUID, error) {
	if c == "" {
		return uuid.Nil, nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(string(c))
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode cursor: %w", errs.ErrValidation)
	}

	version, id, ok := strings.Cut(string(raw), ":")
	if !ok || version != cursorVersion {
		return uuid.Nil, fmt.Errorf("decode cursor: unknown version: %w", errs.ErrValidation)
	}

	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode cursor: %w", errs.ErrValidation)
	}
	return parsed, nil
}
