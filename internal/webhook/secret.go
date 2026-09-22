package webhook

import (
	"context"
	"errors"
	"fmt"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// SecretOpener opens a webhook's sealed signing secret (ADR-0016). It is declared here, consumer-side,
// because the sender is the only thing in the gateway that needs the clear key: it holds it for the time
// it takes to compute one HMAC, and never stores or logs it.
//
// The gateway's first real Open caller. The two secrets step-295 sealed are still never opened — the
// connector pool reads its password from the environment, and the Admin API only ever masks the provider
// credentials — so the failure modes below had no precedent to copy.
type SecretOpener interface {
	Open(ctx context.Context, sealed cp.SealedSecret) ([]byte, error)
}

// noOpener is what a Sender built without an opener gets. It fails every delivery as UNAVAILABLE, which
// makes the events pile up unacknowledged instead of being dead-lettered: a sender with no opener is a
// wiring defect, and a wiring defect must look like a service that makes no progress, not like six hours
// of an account's return traffic quietly landing in the dead-letter.
type noOpener struct{}

func (noOpener) Open(context.Context, cp.SealedSecret) ([]byte, error) {
	return nil, fmt.Errorf("no secret opener configured: %w", errs.ErrServiceUnavailable)
}

// openSecret opens wh's signing secret for one delivery. It is called once per delivery, not once per HTTP
// attempt: the in-band retry loop reuses what it opened.
func (s *Sender) openSecret(ctx context.Context, wh cp.Webhook) (string, error) {
	plaintext, err := s.opener.Open(ctx, wh.Secret)
	if err != nil {
		return "", fmt.Errorf("open signing secret of webhook %s: %w", wh.ID, err)
	}
	return string(plaintext), nil
}

// onOpenFailure routes a failed open, and the distinction it makes is the whole point.
//
// A key service that is down, or a context that ended, is TRANSIENT: the error travels back to the caller,
// which reprocesses its record. No attempt is spent, nothing is deferred, nothing is parked — exactly what
// the callers already do when the control-plane read itself fails.
//
// Everything else is DETERMINISTIC for this row: a tampered or truncated ciphertext, one sealed under a
// master key that is gone, a wrong domain tag, an authorisation the interceptor refuses. Redelivering it
// would fail identically forever and hold the partition — and every other account's MO and DLR queued
// behind it. So the event is dead-lettered, where it stays recoverable once the row is re-sealed, and the
// consumer moves on. The reason carries the cause, never the secret.
func (s *Sender) onOpenFailure(ctx context.Context, wh cp.Webhook, ev Event, err error) error {
	if ctx.Err() != nil || errors.Is(err, errs.ErrServiceUnavailable) {
		return err
	}
	return s.park(ctx, wh, ev, "secret_unopenable: "+err.Error())
}
