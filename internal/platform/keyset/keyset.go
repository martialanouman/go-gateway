// Package keyset encodes a keyset-pagination position — a timestamp and a UUID — as an opaque
// base64url token, for every listing paginated on a (time, id) sort key. The timestamp precision is the
// caller's column precision, never a constant: a cursor finer than the column points between two rows
// and skips one, a coarser one repeats rows sharing an instant.
package keyset

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// Precision is the timestamp resolution a cursor carries.
type Precision int

const (
	// Milli matches ClickHouse DateTime64(3).
	Milli Precision = iota
	// Micro matches PostgreSQL timestamptz.
	Micro
)

// Key is a page position: the sort-key values of the last row served.
type Key struct {
	At time.Time
	ID uuid.UUID
}

// sep separates the two fields inside the payload: a UUID contains no '|' and the timestamp is decimal.
const sep = "|"

// Encode renders k as a token. Tokens issued before this package (three per-caller copies of the same
// format) decode unchanged.
func Encode(k Key, p Precision) string {
	var ts int64
	switch p {
	case Milli:
		ts = k.At.UnixMilli()
	case Micro:
		ts = k.At.UnixMicro()
	default:
		panic(fmt.Sprintf("keyset: unknown precision %d", p))
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(ts, 10) + sep + k.ID.String()))
}

// Decode parses a token produced by Encode at the same precision. A malformed token is an
// errs.ErrValidation: cursors are opaque, so a client should only ever echo one back verbatim.
func Decode(s string, p Precision) (Key, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Key{}, malformed("base64")
	}
	ts, id, ok := strings.Cut(string(raw), sep)
	if !ok {
		return Key{}, malformed("separator")
	}
	n, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return Key{}, malformed("timestamp")
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return Key{}, malformed("id")
	}
	var at time.Time
	switch p {
	case Milli:
		at = time.UnixMilli(n)
	case Micro:
		at = time.UnixMicro(n)
	default:
		panic(fmt.Sprintf("keyset: unknown precision %d", p))
	}
	return Key{At: at.UTC(), ID: parsed}, nil
}

func malformed(part string) error {
	return fmt.Errorf("keyset: malformed cursor (%s): %w", part, errs.ErrValidation)
}
