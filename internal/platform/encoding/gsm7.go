package encoding

// GSM 03.38 7-bit alphabet. A message is GSM-7-encodable iff every rune is in the basic set or the
// escape-prefixed extension set. A basic rune costs one septet; an extension rune costs two (the
// escape 0x1B then the character). These sets are the source of truth for representability and septet
// counting; the actual bit-packing lives with segmentation (step-082).

// gsm7Basic is the 127 printable/basic runes of the default alphabet (the 0x1B escape position is not
// a character and is excluded). Order is irrelevant — it is loaded into a set.
const gsm7Basic = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?¡" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"

// gsm7Extension is the escape-prefixed extension set: form feed, ^, {, }, \, [, ~, ], |, €. Each
// costs two septets.
const gsm7Extension = "\f^{}\\[~]|€"

var (
	gsm7BasicSet     = runeSet(gsm7Basic)
	gsm7ExtensionSet = runeSet(gsm7Extension)
)

func runeSet(s string) map[rune]struct{} {
	set := make(map[rune]struct{})
	for _, r := range s {
		set[r] = struct{}{}
	}
	return set
}

// gsm7Representable reports whether every rune of body is in the GSM-7 alphabet (basic or extension).
func gsm7Representable(body []byte) bool {
	for _, r := range string(body) {
		if !inSet(gsm7BasicSet, r) && !inSet(gsm7ExtensionSet, r) {
			return false
		}
	}
	return true
}

// gsm7Septets returns the septets body occupies once encoded as GSM-7: a basic rune is one septet, an
// extension rune two, and any rune outside the alphabet is substituted by a single '?' (one septet) —
// so a message a client forced to GSM-7 is billed as the GSM-7 it will actually be sent as, never
// mis-counted as UCS-2. Use gsm7Representable first when the goal is detection rather than counting.
func gsm7Septets(body []byte) int {
	septets := 0
	for _, r := range string(body) {
		if inSet(gsm7ExtensionSet, r) {
			septets += 2 // escape + character
		} else {
			septets++ // a basic rune, or a non-representable rune substituted by '?'
		}
	}
	return septets
}

func inSet(set map[rune]struct{}, r rune) bool {
	_, ok := set[r]
	return ok
}
