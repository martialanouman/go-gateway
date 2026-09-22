package webhook

import (
	"context"
	"errors"
	"fmt"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// SecretOpener opens a webhook's sealed signing secret (ADR-0016).
type SecretOpener interface {
	Open(ctx context.Context, sealed cp.SealedSecret) ([]byte, error)
}

func (s *Sender) openSecret(ctx context.Context, wh cp.Webhook) (string, error) {
	plaintext, err := s.opener.Open(ctx, wh.Secret)
	if err != nil {
		return "", fmt.Errorf("open signing secret of webhook %s: %w", wh.ID, err)
	}
	if len(plaintext) == 0 {
		return "", fmt.Errorf("signing secret of webhook %s opened empty: %w", wh.ID, errs.ErrInternal)
	}
	return string(plaintext), nil
}

// Transient failures go back to the caller, which redelivers. Anything deterministic for this row is
// dead-lettered instead: redelivered, it would hold the partition for every other account too.
func (s *Sender) onOpenFailure(ctx context.Context, wh cp.Webhook, ev Event, err error) error {
	// ctx.Err() is not redundant with the opener's classification: SecretOpener is a consumer-side interface
	// and the next implementer need not translate cancellation into ErrServiceUnavailable. Without this, an
	// opener returning a bare ctx.Err() would dead-letter every in-flight event on SIGTERM.
	if ctx.Err() != nil || errors.Is(err, errs.ErrServiceUnavailable) {
		return err
	}
	return s.park(ctx, wh, ev, "secret_unopenable: "+err.Error())
}
