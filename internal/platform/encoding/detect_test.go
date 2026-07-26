package encoding_test

import (
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/platform/encoding"
)

func iptr(i int) *int { return &i }

func TestDetectAndCount(t *testing.T) {
	cases := []struct {
		name      string
		requested string
		dcd       *int
		body      string
		wantEnc   string
		wantSegs  int
	}{
		// --- GSM-7 boundaries (160 single, 153 multi) ---
		{"empty is one gsm7 segment", "auto", nil, "", encoding.GSM7, 1},
		{"gsm7 160 -> 1 seg", "auto", nil, strings.Repeat("a", 160), encoding.GSM7, 1},
		{"gsm7 161 -> 2 segs at 153", "auto", nil, strings.Repeat("a", 161), encoding.GSM7, 2},
		{"gsm7 306 (2*153) -> 2 segs", "auto", nil, strings.Repeat("a", 306), encoding.GSM7, 2},
		{"gsm7 307 -> 3 segs", "auto", nil, strings.Repeat("a", 307), encoding.GSM7, 3},

		// --- GSM-7 extension chars count as two septets ---
		{"80 euro signs = 160 septets -> 1 seg", "auto", nil, strings.Repeat("€", 80), encoding.GSM7, 1},
		{"81 euro signs = 162 septets -> 2 segs", "auto", nil, strings.Repeat("€", 81), encoding.GSM7, 2},
		{"braces are extension chars", "auto", nil, strings.Repeat("{", 80), encoding.GSM7, 1},

		// --- UCS-2 boundaries (70 single, 67 multi) ---
		{"non-gsm char -> ucs2", "auto", nil, "ю", encoding.UCS2, 1},
		{"ucs2 70 -> 1 seg", "auto", nil, strings.Repeat("ю", 70), encoding.UCS2, 1},
		{"ucs2 71 -> 2 segs at 67", "auto", nil, strings.Repeat("ю", 71), encoding.UCS2, 2},
		{"emoji is a surrogate pair (2 units)", "auto", nil, strings.Repeat("😀", 35), encoding.UCS2, 1},
		{"36 emoji = 72 units -> 2 segs", "auto", nil, strings.Repeat("😀", 36), encoding.UCS2, 2},

		// --- explicit client override wins over content ---
		{"forced ucs2 on ascii", "ucs2", nil, "hello", encoding.UCS2, 1},
		// A forced GSM-7 on non-representable content stays GSM-7 (each bad rune → '?' = one septet), so
		// the label and the septet count are consistent — never a silent UCS-2 promotion nor a UCS-2 count.
		{"forced gsm7 on non-representable stays gsm7", "gsm7", nil, "юю", encoding.GSM7, 1},
		{"forced gsm7 161 substituted runes -> 2 segs", "gsm7", nil, strings.Repeat("ю", 161), encoding.GSM7, 2},
		{"forced binary boundary 140 -> 1", "binary", nil, strings.Repeat("x", 140), encoding.Binary, 1},
		{"forced binary 141 -> 2 at 134", "binary", nil, strings.Repeat("x", 141), encoding.Binary, 2},

		// --- connector data_coding_default honoured when the client says auto ---
		{"data_coding 8 (ucs2) forces ucs2", "auto", iptr(0x08), "hello", encoding.UCS2, 1},
		{"data_coding 0 (default alphabet) is gsm7", "auto", iptr(0x00), "hello", encoding.GSM7, 1},
		{"client override beats data_coding_default", "gsm7", iptr(0x08), "hello", encoding.GSM7, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enc, segs := encoding.DetectAndCount(tc.requested, tc.dcd, []byte(tc.body))
			if enc != tc.wantEnc {
				t.Errorf("encoding = %q, want %q", enc, tc.wantEnc)
			}
			if segs != tc.wantSegs {
				t.Errorf("segments = %d, want %d", segs, tc.wantSegs)
			}
		})
	}
}

// FuzzDetect reinforces the total-function guarantee: whatever the input, DetectAndCount never
// panics, resolves to one of the three concrete encodings, and always reports at least one segment.
func FuzzDetect(f *testing.F) {
	f.Add("auto", "hello")
	f.Add("gsm7", "€{}[]")
	f.Add("ucs2", "ю😀")
	f.Add("binary", "\x00\x01\xff")
	f.Add("", strings.Repeat("a", 1000))

	f.Fuzz(func(t *testing.T, requested, body string) {
		enc, segs := encoding.DetectAndCount(requested, nil, []byte(body))
		switch enc {
		case encoding.GSM7, encoding.UCS2, encoding.Binary:
		default:
			t.Fatalf("encoding %q is not one of gsm7/ucs2/binary", enc)
		}
		if segs < 1 {
			t.Fatalf("segment count %d < 1 for body of %d bytes", segs, len(body))
		}
	})
}
