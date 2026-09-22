package webhook

import (
	"context"
	"errors"
	"fmt"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

type SecretOpener interface {
	Open(ctx context.Context, sealed cp.SealedSecret) ([]byte, error)
}

type noOpener struct{}

// Unavailable and not a dead-letter: a missing opener is a wiring defect, which must look like a service
// that makes no progress, not like a backlog quietly parked.
func (noOpener) Open(context.Context, cp.SealedSecret) ([]byte, error) {
	return nil, fmt.Errorf("no secret opener configured: %w", errs.ErrServiceUnavailable)
}

func (s *Sender) openSecret(ctx context.Context, wh cp.Webhook) (string, error) {
	plaintext, err := s.opener.Open(ctx, wh.Secret)
	if err != nil {
		return "", fmt.Errorf("open signing secret of webhook %s: %w", wh.ID, err)
	}
	return string(plaintext), nil
}

// Transient failures go back to the caller, which redelivers. Anything deterministic for this row is
// dead-lettered instead: redelivered, it would hold the partition for every other account too.
func (s *Sender) onOpenFailure(ctx context.Context, wh cp.Webhook, ev Event, err error) error {
	if ctx.Err() != nil || errors.Is(err, errs.ErrServiceUnavailable) {
		return err
	}
	return s.park(ctx, wh, ev, "secret_unopenable: "+err.Error())
}
