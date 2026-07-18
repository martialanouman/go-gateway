package smpp

import (
	"bytes"
	"testing"
)

func TestConcatUDHRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		c    Concat
	}{
		{"8-bit ref", Concat{Reference: 0x42, Total: 3, Sequence: 2}},
		{"16-bit ref", Concat{Reference: 0xBEEF, Total: 4, Sequence: 1, Ref16: true}},
	}
	content := []byte("segment body")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sm := EncodeConcatUDH(tc.c, content)
			got, body, hasConcat, err := ParseUDH(sm)
			if err != nil {
				t.Fatalf("ParseUDH: %v", err)
			}
			if !hasConcat {
				t.Fatal("expected a concatenation element")
			}
			if got != tc.c {
				t.Errorf("concat: got %+v want %+v", got, tc.c)
			}
			if !bytes.Equal(body, content) {
				t.Errorf("content: got %q want %q", body, content)
			}
		})
	}
}

func TestParseUDHRejectsTruncated(t *testing.T) {
	tests := [][]byte{
		{},                 // empty
		{0x05, 0x00},       // UDHL claims 5 octets, only 1 present
		{0x03, 0x00, 0x03}, // IE claims IEDL 3, but only 1 octet of data follows
	}
	for _, data := range tests {
		if _, _, _, err := ParseUDH(data); err == nil {
			t.Errorf("ParseUDH(%x): expected error", data)
		}
	}
}

func TestParseUDHNoConcatElement(t *testing.T) {
	// A valid header carrying only an unrelated IE (0x24 = hyperlink) must parse without a concat.
	sm := []byte{0x03, 0x24, 0x01, 0x00, 'h', 'i'}
	_, body, hasConcat, err := ParseUDH(sm)
	if err != nil {
		t.Fatalf("ParseUDH: %v", err)
	}
	if hasConcat {
		t.Error("did not expect a concatenation element")
	}
	if string(body) != "hi" {
		t.Errorf("content: got %q", body)
	}
}
