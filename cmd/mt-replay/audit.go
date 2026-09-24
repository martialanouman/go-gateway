package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

const (
	maxOperatorLen = 64
	auditTimeout   = 5 * time.Second
)

// auditTrail is the slice of *postgres.AuditLogRepo the replay writes to.
type auditTrail interface {
	Begin(ctx context.Context, in cp.AuditIntent) (uuid.UUID, error)
	Finish(ctx context.Context, id uuid.UUID, status int) error
}

// declaredOperator prefixes the name so a self-declared identity never reads as an authenticated
// fingerprint (tok_…) in the trail.
func declaredOperator(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("-operator is required: a replay is recorded under the name of who ran it")
	}
	if utf8.RuneCountInString(name) > maxOperatorLen {
		return "", fmt.Errorf("-operator is longer than %d characters", maxOperatorLen)
	}
	if strings.IndexFunc(name, func(r rune) bool { return !unicode.IsPrint(r) }) >= 0 {
		return "", errors.New("-operator holds a non-printable character")
	}
	return "declared:" + name, nil
}

// auditedReplay applies the Admin API's rule to the tool: no row, no replay.
func auditedReplay(ctx context.Context, trail auditTrail, operator, runID string, drain func(context.Context) error) error {
	beginCtx, cancel := context.WithTimeout(ctx, auditTimeout)
	id, err := trail.Begin(beginCtx, cp.AuditIntent{
		Operator:    operator,
		OperationID: serviceName,
		Method:      "REPLAY",
		Target:      "mt.dead-letter",
		RequestID:   runID,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("audit intent not recorded; nothing replayed: %w", err)
	}

	err = drain(ctx)
	status := http.StatusOK
	if err != nil {
		status = http.StatusInternalServerError
	}
	// ctx is already cancelled by the signal that stopped the drain.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), auditTimeout)
	defer cancel()
	if ferr := trail.Finish(finishCtx, id, status); ferr != nil {
		slog.WarnContext(ctx, "audit outcome not recorded", "audit_id", id, "run_id", runID, "err", ferr)
	}
	return err
}
