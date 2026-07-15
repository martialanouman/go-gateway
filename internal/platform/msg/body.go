// Package msg holds the message content primitives shared by the MT and MO pipelines.
package msg

import (
	"log/slog"
)

// Redacted is the placeholder every rendering of a Body yields in place of the plaintext.
const Redacted = "[REDACTED]"

// Body wraps message content so it cannot leak. Every rendering path a body could plausibly
// escape through — %v/%s via String, encoding/json via MarshalJSON, log/slog via LogValue,
// and OTel span attributes (which stringify through String) — yields Redacted instead of the
// plaintext. The plaintext is reachable only through Reveal, which is explicit, greppable and
// audited at its call sites. This is invariant (a): the body never appears in a log or a span,
// under any storage policy or environment (guide de codage §11, spec §6.11/§6.23).
//
// The zero value is a valid empty body. Body is a value type; copies are safe.
type Body struct {
	b []byte
}

// NewBody wraps plaintext content in a Body. It takes ownership of b: callers must not retain
// or mutate the slice afterwards, since Body does not copy it on the hot path.
func NewBody(b []byte) Body {
	return Body{b: b}
}

// NewBodyString wraps a plaintext string in a Body.
func NewBodyString(s string) Body {
	return Body{b: []byte(s)}
}

// String satisfies fmt.Stringer with the redacted placeholder, so a Body caught by %v, %s or a
// span attribute cannot print its content. It deliberately has a value receiver so that both
// Body and *Body are redacted.
func (Body) String() string {
	return Redacted
}

// MarshalJSON satisfies json.Marshaler with the redacted placeholder, so a struct embedding a
// Body cannot leak it through encoding/json — including slog's JSON handler.
func (Body) MarshalJSON() ([]byte, error) {
	return []byte(`"` + Redacted + `"`), nil
}

// LogValue satisfies slog.LogValuer with the redacted placeholder. slog resolves LogValue before
// reaching any handler, so the plaintext never enters a log record whatever the handler is.
func (Body) LogValue() slog.Value {
	return slog.StringValue(Redacted)
}

// Reveal returns the plaintext. It is the ONLY way out of a Body: every call site is an audited
// exception to invariant (a) and must be justified (encoding, segmentation, SMSC submission,
// encryption at rest). Never pass the result to a logger, a span attribute or a metric label.
func (b Body) Reveal() []byte {
	return b.b
}

// Len returns the plaintext length in bytes. It is safe to log: a length is not content.
func (b Body) Len() int {
	return len(b.b)
}

// IsEmpty reports whether the body carries no content.
func (b Body) IsEmpty() bool {
	return len(b.b) == 0
}
