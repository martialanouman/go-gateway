package uuidx_test

import (
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/platform/uuidx"
)

func TestNewReturnsV7(t *testing.T) {
	id := uuidx.New()

	if id == uuid.Nil {
		t.Fatal("New() returned the nil UUID")
	}
	if got := id.Version(); got != 7 {
		t.Errorf("version = %d, want 7", got)
	}
	if got := id.Variant(); got != uuid.RFC4122 {
		t.Errorf("variant = %v, want RFC4122", got)
	}
	if !uuidx.IsV7(id) {
		t.Error("IsV7() = false for a freshly generated v7")
	}
}

func TestNewIsUnique(t *testing.T) {
	const n = 10_000
	seen := make(map[uuid.UUID]struct{}, n)

	for range n {
		id := uuidx.New()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %s within %d draws", id, n)
		}
		seen[id] = struct{}{}
	}
}

// TestNewIsMonotonicallySortable pins the property the DDL relies on: v7 ids minted in sequence
// sort in generation order, keeping index inserts append-only.
func TestNewIsMonotonicallySortable(t *testing.T) {
	const n = 1_000
	prev := uuidx.New()

	for i := 1; i < n; i++ {
		id := uuidx.New()
		if id.String() <= prev.String() {
			t.Fatalf("id %d (%s) does not sort after its predecessor (%s)", i, id, prev)
		}
		prev = id
	}
}

// TestNewIsRaceFree guards concurrent minting: every service issues ids from many goroutines.
func TestNewIsRaceFree(t *testing.T) {
	const goroutines, per = 8, 500

	var (
		mu   sync.Mutex
		seen = make(map[uuid.UUID]struct{}, goroutines*per)
		wg   sync.WaitGroup
	)

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			ids := make([]uuid.UUID, 0, per)
			for range per {
				ids = append(ids, uuidx.New())
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range ids {
				seen[id] = struct{}{}
			}
		}()
	}
	wg.Wait()

	if len(seen) != goroutines*per {
		t.Errorf("got %d unique ids, want %d — collision under concurrency", len(seen), goroutines*per)
	}
}

func TestNewE(t *testing.T) {
	id, err := uuidx.NewE()
	if err != nil {
		t.Fatalf("NewE() error = %v", err)
	}
	if !uuidx.IsV7(id) {
		t.Errorf("NewE() version = %d, want 7", id.Version())
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{"canonical v7", "0199a1b2-c3d4-7000-8000-000000000001", false},
		{"round-tripped", uuidx.New().String(), false},
		{"uppercase", "0199A1B2-C3D4-7000-8000-000000000001", false},
		{"empty", "", true},
		{"garbage", "not-a-uuid", true},
		{"truncated", "0199a1b2-c3d4-7000-8000", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := uuidx.Parse(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Parse(%q) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) error = %v", tc.in, err)
			}
			if got == uuid.Nil {
				t.Errorf("Parse(%q) returned the nil UUID", tc.in)
			}
		})
	}
}

func TestIsV7RejectsOtherVersions(t *testing.T) {
	v4 := uuid.New() // v4: random, not time-ordered

	if uuidx.IsV7(v4) {
		t.Error("IsV7() = true for a v4 UUID")
	}
	if uuidx.IsV7(uuid.Nil) {
		t.Error("IsV7() = true for the nil UUID")
	}
}
