package encoding_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	pipeenc "github.com/martialanouman/go-gateway/internal/pipeline/encoding"
	platenc "github.com/martialanouman/go-gateway/internal/platform/encoding"
	"github.com/martialanouman/go-gateway/internal/smpp"
)

// TestSplitSingleSegmentHasNoUDH: a short message is one segment carrying the bare content, with no
// User Data Header.
func TestSplitSingleSegmentHasNoUDH(t *testing.T) {
	segs := pipeenc.Split(uuid.New(), []byte("hello"), platenc.GSM7)
	if len(segs) != 1 {
		t.Fatalf("segments = %d, want 1", len(segs))
	}
	s := segs[0]
	if s.HasUDH || s.Seq != 1 || s.Total != 1 {
		t.Errorf("segment = %+v, want seq 1 / total 1 / no UDH", s)
	}
	if string(s.Payload) != "hello" {
		t.Errorf("payload = %q, want the bare content", s.Payload)
	}
}

// TestSplitGSM7_161IntoTwo is the acceptance case: 161 GSM-7 chars split into 152 + 9 (the 16-bit-ref
// concatenation UDH costs 8 septets, leaving 152 per segment), each carrying a well-formed
// concatenation UDH that round-trips through ParseUDH.
func TestSplitGSM7_161IntoTwo(t *testing.T) {
	body := strings.Repeat("a", 161)
	segs := pipeenc.Split(uuid.New(), []byte(body), platenc.GSM7)
	if len(segs) != 2 {
		t.Fatalf("segments = %d, want 2", len(segs))
	}

	want := []int{152, 9}
	for i, s := range segs {
		if !s.HasUDH {
			t.Errorf("segment %d must carry a UDH", i+1)
		}
		concat, content, hasConcat, err := smpp.ParseUDH(s.Payload)
		if err != nil {
			t.Fatalf("ParseUDH segment %d: %v", i+1, err)
		}
		if !hasConcat {
			t.Errorf("segment %d has no concatenation IE", i+1)
		}
		if int(concat.Total) != 2 || int(concat.Sequence) != i+1 || !concat.Ref16 {
			t.Errorf("segment %d concat = %+v, want total 2 / seq %d / 16-bit ref", i+1, concat, i+1)
		}
		if len(content) != want[i] {
			t.Errorf("segment %d content length = %d, want %d", i+1, len(content), want[i])
		}
		// All segments share one reference.
		if concat.Reference != segs[0].Ref {
			t.Errorf("segment %d reference %d differs from %d", i+1, concat.Reference, segs[0].Ref)
		}
	}
}

// TestSplitUCS2Boundary: a UCS-2 message of 71 units splits into 66 + 5 (each unit is 2 octets on the
// wire; the 7-octet UDH leaves 133 octets = 66 units per segment).
func TestSplitUCS2Boundary(t *testing.T) {
	segs := pipeenc.Split(uuid.New(), []byte(strings.Repeat("ю", 71)), platenc.UCS2)
	if len(segs) != 2 {
		t.Fatalf("segments = %d, want 2", len(segs))
	}
	_, c1, _, _ := smpp.ParseUDH(segs[0].Payload)
	_, c2, _, _ := smpp.ParseUDH(segs[1].Payload)
	if len(c1) != 66*2 || len(c2) != 5*2 {
		t.Errorf("ucs2 segment octet lengths = %d/%d, want %d/%d", len(c1), len(c2), 66*2, 5*2)
	}
}

// TestSplitDeterministic: the same (messageID, body) always splits into byte-for-byte identical
// segments — the concatenation reference is derived from the id, so a replay reassembles correctly.
func TestSplitDeterministic(t *testing.T) {
	id := uuid.New()
	body := []byte(strings.Repeat("x", 400))
	a := pipeenc.Split(id, body, platenc.GSM7)
	b := pipeenc.Split(id, body, platenc.GSM7)
	if len(a) != len(b) {
		t.Fatalf("segment counts differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Ref != b[i].Ref || string(a[i].Payload) != string(b[i].Payload) {
			t.Errorf("segment %d differs across identical splits", i+1)
		}
	}
	// A different id yields a different reference (collision-resistant grouping).
	if c := pipeenc.Split(uuid.New(), body, platenc.GSM7); c[0].Ref == a[0].Ref {
		t.Log("note: references collided across two random ids (a 1/65536 chance) — not a failure")
	}
}

// FuzzSplit reinforces robustness: whatever the encoding and body, Split never panics, always yields
// at least one segment, numbers them 1..N sharing one Total, and every multi-segment payload begins
// with a parseable UDH.
func FuzzSplit(f *testing.F) {
	f.Add("gsm7", "hello")
	f.Add("ucs2", "ю😀")
	f.Add("binary", "\x00\xff")
	f.Add("gsm7", strings.Repeat("€", 200))

	f.Fuzz(func(t *testing.T, enc, body string) {
		segs := pipeenc.Split(uuid.New(), []byte(body), enc)
		if len(segs) < 1 {
			t.Fatalf("no segments for %q body of %d bytes", enc, len(body))
		}
		for i, s := range segs {
			if s.Seq != i+1 || s.Total != len(segs) {
				t.Fatalf("segment %d numbering = seq %d / total %d, want %d / %d", i, s.Seq, s.Total, i+1, len(segs))
			}
			if s.HasUDH {
				if _, _, _, err := smpp.ParseUDH(s.Payload); err != nil {
					t.Fatalf("segment %d UDH does not parse: %v", i+1, err)
				}
			}
		}
	})
}

// TestSplitCountMatchesDetect: the number of segments Split produces equals what DetectAndCount
// reports, across encodings and boundaries — the two must never disagree.
func TestSplitCountMatchesDetect(t *testing.T) {
	cases := []struct {
		enc  string
		body string
	}{
		{platenc.GSM7, ""},
		{platenc.GSM7, strings.Repeat("a", 160)},
		{platenc.GSM7, strings.Repeat("a", 161)},
		{platenc.GSM7, strings.Repeat("€", 81)},  // extension chars count double
		{platenc.GSM7, strings.Repeat("€", 153)}, // exactly fills segments (76 per) then spills
		{platenc.UCS2, strings.Repeat("ю", 70)},
		{platenc.UCS2, strings.Repeat("ю", 71)},
		{platenc.UCS2, strings.Repeat("😀", 36)}, // surrogate pairs
		{platenc.UCS2, strings.Repeat("😀", 67)},
		{platenc.Binary, strings.Repeat("x", 140)},
		{platenc.Binary, strings.Repeat("x", 141)},
		// A mixed body where each segment packs to 151 septets (75 euro signs + one 'a') and the next
		// extension char straddles the boundary: greedy needs 152 segments where a naive ceil(total/152)
		// would report 151. Split can only pack greedily (a rune never splits), so DetectAndCount must
		// too — this case fails the moment its count regresses to a ceiling division.
		{platenc.GSM7, strings.Repeat(strings.Repeat("€", 75)+"a", 152)},
	}
	for _, tc := range cases {
		_, wantSegs := platenc.DetectAndCount(tc.enc, nil, []byte(tc.body))
		gotSegs := len(pipeenc.Split(uuid.New(), []byte(tc.body), tc.enc))
		if gotSegs != wantSegs {
			t.Errorf("%s len %d: Split -> %d segments, DetectAndCount -> %d", tc.enc, len(tc.body), gotSegs, wantSegs)
		}
	}
}
