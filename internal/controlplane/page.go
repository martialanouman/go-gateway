package controlplane

// Page is one page of a cursor-paginated listing. NextCursor is empty when Items is the last page;
// HasMore says the same thing in the boolean the contract's PageMeta exposes. The two are kept
// consistent by the repository that builds the page.
type Page[T any] struct {
	Items      []T
	NextCursor Cursor
	HasMore    bool
}
