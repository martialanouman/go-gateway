package postgres

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// This file is the single place the storage layer bridges the small type gaps between the
// sqlc-generated params/models and the controlplane domain types: int32 vs int, a *string column
// vs a named enum type, and (for connectors) jsonb. Keeping the conversions here means no pgtype or
// int32 ever escapes the repository boundary.

// i32ptr narrows a *int to a *int32 for a query parameter. The control-plane integer columns
// (limits, retention days, floors) are small by construction and the API caps them, so the
// narrowing cannot overflow in practice.
func i32ptr(p *int) *int32 {
	if p == nil {
		return nil
	}
	v := int32(*p) //nolint:gosec // G115: bounded control-plane integers; see doc comment.
	return &v
}

// intptr widens a *int32 from a row to a *int for a domain type.
func intptr(p *int32) *int {
	if p == nil {
		return nil
	}
	v := int(*p)
	return &v
}

// i16ptr narrows a *int to a *int16 for a smallint query parameter. The connector SMPP-parameter
// columns are smallint; the domain and API expose them as int. The values are all small protocol
// constants, so the narrowing cannot overflow in practice.
func i16ptr(p *int) *int16 {
	if p == nil {
		return nil
	}
	v := int16(*p) //nolint:gosec // G115: small SMPP protocol constants; see doc comment.
	return &v
}

// int16ptr widens a *int16 from a smallint row to a *int for a domain type.
func int16ptr(p *int16) *int {
	if p == nil {
		return nil
	}
	v := int(*p)
	return &v
}

// jsonbBytes marshals a domain map to the []byte a jsonb parameter takes. A nil map becomes SQL
// NULL (nil bytes), not an empty JSON object.
func jsonbBytes(m map[string]any) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return json.Marshal(m)
}

// jsonbMap unmarshals a jsonb column into a domain map. Empty bytes (SQL NULL) become a nil map.
func jsonbMap(b []byte) (map[string]any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// enumPtr projects a *string column onto a named string-enum pointer for a domain type.
func enumPtr[T ~string](p *string) *T {
	if p == nil {
		return nil
	}
	v := T(*p)
	return &v
}

// strPtr projects a named string-enum pointer onto a *string query parameter.
func strPtr[T ~string](p *T) *string {
	if p == nil {
		return nil
	}
	v := string(*p)
	return &v
}

// afterPtr turns a keyset position into a nullable query parameter: the nil UUID (first page) means
// "no lower bound".
func afterPtr(id uuid.UUID) *uuid.UUID {
	if id == uuid.Nil {
		return nil
	}
	return &id
}

// tsVal reads a NOT NULL timestamptz column into a time.Time.
func tsVal(ts pgtype.Timestamptz) time.Time { return ts.Time }

// tsFrom builds a valid timestamptz parameter from a time.Time.
func tsFrom(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }

// tsFromPtr builds a nullable timestamptz parameter from a *time.Time (SQL NULL when nil).
func tsFromPtr(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

// tsPtr reads a nullable timestamptz column into a *time.Time (nil when SQL NULL).
func tsPtr(ts pgtype.Timestamptz) *time.Time {
	if !ts.Valid {
		return nil
	}
	v := ts.Time
	return &v
}

// numFloat reads a NOT NULL numeric column into a float64. The connector's reconnect_multiplier is
// numeric(4,2); the domain and the contract expose it as a plain number.
func numFloat(n pgtype.Numeric) float64 {
	f, err := n.Float64Value()
	if err != nil || !f.Valid {
		return 0
	}
	return f.Float64
}
