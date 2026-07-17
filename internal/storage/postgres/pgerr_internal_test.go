package postgres

import (
	goerrors "errors"
	"fmt"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// TestTranslateMapsSQLSTATEToTheCatalogue is the boundary contract of the repository layer: every
// class of driver error must reach the rest of the system as an errs.Code, never as a raw pgx
// error. The credential-cardinality 409 that M1's acceptance criteria hinge on is the
// unique-violation row here.
func TestTranslateMapsSQLSTATEToTheCatalogue(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want errs.Code
	}{
		{"no rows is not found", pgx.ErrNoRows, errs.ErrNotFound},
		{"wrapped no rows is not found", fmt.Errorf("query: %w", pgx.ErrNoRows), errs.ErrNotFound},
		{"unique violation is conflict", &pgconn.PgError{Code: pgerrcode.UniqueViolation}, errs.ErrConflict},
		{"foreign key violation is validation", &pgconn.PgError{Code: pgerrcode.ForeignKeyViolation}, errs.ErrValidation},
		{"check violation is validation", &pgconn.PgError{Code: pgerrcode.CheckViolation}, errs.ErrValidation},
		{"unknown sqlstate is internal", &pgconn.PgError{Code: pgerrcode.SyntaxError}, errs.ErrInternal},
		{"opaque error is internal", goerrors.New("connection refused"), errs.ErrInternal},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := translate("do thing", tc.err)
			code, ok := errs.CodeOf(got)
			if !ok {
				t.Fatalf("translate() produced no code: %v", got)
			}
			if code != tc.want {
				t.Errorf("translate() code = %q, want %q", code, tc.want)
			}
			if !goerrors.Is(got, tc.want) {
				t.Errorf("translate() result is not errors.Is %q", tc.want)
			}
		})
	}
}

// TestTranslatePassesNilThrough: a successful operation must not be turned into an error.
func TestTranslateNilIsNil(t *testing.T) {
	if got := translate("do thing", nil); got != nil {
		t.Errorf("translate(nil) = %v, want nil", got)
	}
}

// TestTranslateKeepsTheOperationContext: the code is machine-readable, but the wrapped message must
// still name the failing operation for the log.
func TestTranslateKeepsTheOperationContext(t *testing.T) {
	got := translate("create customer", &pgconn.PgError{Code: pgerrcode.UniqueViolation})
	if msg := got.Error(); msg == "" || msg[:15] != "create customer" {
		t.Errorf("translate() lost the operation context: %q", got)
	}
}
