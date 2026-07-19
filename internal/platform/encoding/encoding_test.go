package encoding_test

import (
	"testing"

	"github.com/martialanouman/go-gateway/internal/platform/encoding"
)

func TestFromDataCoding(t *testing.T) {
	tests := []struct {
		name string
		dc   uint8
		want string
	}{
		{"default alphabet is gsm7", 0x00, encoding.GSM7},
		{"8-bit is binary", 0x04, encoding.Binary},
		{"ucs2", 0x08, encoding.UCS2},
		{"message class default alphabet is gsm7", 0xF0, encoding.GSM7},
		{"message class 8-bit is binary", 0xF4, encoding.Binary},
		{"unknown coding falls back to gsm7", 0x1C, encoding.GSM7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := encoding.FromDataCoding(tc.dc); got != tc.want {
				t.Errorf("FromDataCoding(%#x) = %q, want %q", tc.dc, got, tc.want)
			}
		})
	}
}
