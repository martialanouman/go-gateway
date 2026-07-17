package postgres

import (
	"errors"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// translate maps a driver error to the gateway's error catalogue. It is the ONLY place in the
// repository layer that knows a PostgreSQL SQLSTATE: above this line an error carries an errs.Code
// and nothing else, so the HTTP boundary can map it without ever touching pgx.
//
// op names the operation for the wrapped message ("create customer"), which becomes the human
// context while the sentinel carries the machine code. A nil error passes through as nil.
func translate(op string, err error) error {
	if err == nil {
		return nil
	}

	// No rows on a single-row query is a missing resource, not a failure.
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, errs.ErrNotFound)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgerrcode.UniqueViolation:
			// A duplicate against a UNIQUE constraint — the credential-cardinality rule, a duplicate
			// account name, a reused connector name — is a client conflict (409), not a server fault.
			return fmt.Errorf("%s: %w", op, errs.ErrConflict)
		case pgerrcode.ForeignKeyViolation:
			// Referencing a row that does not exist (an account under an unknown customer) is bad
			// input: 422, matching the contract, which lists no 404 on those creates.
			return fmt.Errorf("%s: %w", op, errs.ErrValidation)
		case pgerrcode.CheckViolation:
			// A CHECK the handler did not pre-validate (a channel rule, a shape rule) is semantic
			// validation failure: 422.
			return fmt.Errorf("%s: %w", op, errs.ErrValidation)
		}
	}

	// Anything else is an unexpected infrastructure failure: 500, retryable, wrapped for the log.
	return fmt.Errorf("%s: %w", op, errors.Join(err, errs.ErrInternal))
}
