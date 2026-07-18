package smpp

import (
	"bytes"
	"testing"
)

// FuzzUnmarshal drives the decoder with arbitrary bytes. The decoder parses input that arrives
// from a remote peer (strategie-de-test §4.2), so a panic on malformed input is a security bug:
// the only acceptable outcomes are a clean error or a valid PDU. When a PDU does decode, the codec
// must be canonical — re-encoding it and decoding again yields the identical bytes.
func FuzzUnmarshal(f *testing.F) {
	for _, p := range samplePDUs() {
		if b, err := Marshal(p); err == nil {
			f.Add(b)
		}
	}
	// A few hand-crafted seeds around the header boundary.
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 16})
	f.Add(bytes.Repeat([]byte{0xFF}, 16))

	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := Unmarshal(data)
		if err != nil {
			return // a clean rejection is the expected outcome for most inputs
		}
		b1, err := Marshal(p)
		if err != nil {
			t.Fatalf("re-marshal of a decoded pdu failed: %v", err)
		}
		p2, err := Unmarshal(b1)
		if err != nil {
			t.Fatalf("re-decode of a re-marshalled pdu failed: %v", err)
		}
		b2, err := Marshal(p2)
		if err != nil {
			t.Fatalf("second re-marshal failed: %v", err)
		}
		if !bytes.Equal(b1, b2) {
			t.Fatalf("codec is not canonical:\n first: %x\nsecond: %x", b1, b2)
		}
	})
}
