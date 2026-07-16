package postgres

import (
	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

// paginate turns a slice fetched with limit+1 rows into a page. When more than limit rows came
// back, a further page exists: the extra row is dropped and the cursor points at the last kept
// row's id. id extracts the keyset column from an item.
func paginate[T any](items []T, limit int, id func(T) uuid.UUID) cp.Page[T] {
	if limit > 0 && len(items) > limit {
		last := items[limit-1]
		return cp.Page[T]{
			Items:      items[:limit],
			NextCursor: cp.EncodeCursor(id(last)),
			HasMore:    true,
		}
	}
	return cp.Page[T]{Items: items, HasMore: false}
}
