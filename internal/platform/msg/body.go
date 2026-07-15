// Package msg holds the message content primitives shared by the MT and MO pipelines.
package msg

import (
	"fmt"
	"io"
	"log/slog"
)

// Redacted is the placeholder every rendering of a Body yields in place of the plaintext.
const Redacted = "[REDACTED]"

// Body wraps message content so it cannot leak. Every rendering path a body could plausibly
// escape through — the whole fmt family via Format, encoding/json via MarshalJSON, log/slog via
// LogValue, and OTel span attributes (which stringify through String) — yields Redacted instead
// of the plaintext. The plaintext is reachable only through Reveal, which is explicit, greppable
// and audited at its call sites. This is invariant (a): the body never appears in a log or a span,
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

// Format satisfies fmt.Formatter with the redacted placeholder, and is what closes the fmt family
// completely. String alone is not enough: fmt reaches Stringer only for the string-ish verbs
// (%v, %s, %q, %x, %X), consults GoStringer for %#v, and nothing at all for %d — so those last two
// would fall through to reflection and print the plaintext bytes as hex or decimal. Formatter is
// consulted first and for every verb (%T and %p aside, which render the type and address, not the
// content), including when a Body sits as a field of a struct being dumped. Every verb therefore
// yields Redacted, whatever a caller asks for.
func (Body) Format(f fmt.State, _ rune) {
	// The write cannot fail usefully: the sink is the caller's buffer, and a Body has no error
	// channel to report on.
	_, _ = io.WriteString(f, Redacted)
}

// String satisfies fmt.Stringer with the redacted placeholder, for the consumers that type-assert
// it directly rather than going through fmt — OTel span attributes (attribute.Stringer) and other
// loggers. It deliberately has a value receiver so that both Body and *Body are redacted.
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
