package cancel_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/cancel"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

// TestRedisFlagsMarkExists exercises the real Redis flag store: a marked message reports Exists,
// an unmarked one does not. It runs against a throwaway Redis (real SET/EXISTS semantics).
func TestRedisFlagsMarkExists(t *testing.T) {
	rdb := redistest.Client(t)
	flags := cancel.NewRedisFlags(rdb)
	ctx := context.Background()

	marked := uuid.New()
	if err := flags.Mark(ctx, marked); err != nil {
		t.Fatalf("Mark: %v", err)
	}

	got, err := flags.Exists(ctx, marked)
	if err != nil {
		t.Fatalf("Exists(marked): %v", err)
	}
	if !got {
		t.Error("a marked message must report Exists")
	}

	got, err = flags.Exists(ctx, uuid.New())
	if err != nil {
		t.Fatalf("Exists(unmarked): %v", err)
	}
	if got {
		t.Error("an unmarked message must not report Exists")
	}
}
