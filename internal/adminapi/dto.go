package adminapi

import (
	"github.com/google/uuid"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// PageMeta mirrors the contract's PageMeta. next_cursor is always emitted (explicitly null on the
// last page) so a client may branch on presence or on null; has_more says the same in a boolean.
//
// It is exported on purpose: Huma's $schema transformer rebuilds a response struct copying only its
// exported fields, and an embedded UNEXPORTED type is skipped there — which silently dropped
// next_cursor and has_more from the serialized body. An exported embedded type is promoted
// correctly.
type PageMeta struct {
	NextCursor *string `json:"next_cursor,omitempty" nullable:"true"`
	HasMore    bool    `json:"has_more"`
}

// idString renders a UUID for a response DTO.
func idString(id uuid.UUID) string { return id.String() }

// idPtr renders a nullable UUID for a response DTO: nil stays nil.
func idPtr(id *uuid.UUID) *string {
	if id == nil {
		return nil
	}
	s := id.String()
	return &s
}

// parseIDPtr parses an optional UUID from a request DTO. An empty pointer stays nil; a malformed
// value is a 422 naming the field.
func parseIDPtr(field string, s *string) (*uuid.UUID, error) {
	if s == nil {
		return nil, nil
	}
	id, err := uuid.Parse(*s)
	if err != nil {
		return nil, humaerr.FailValidation("invalid uuid",
			humaerr.FieldError{Field: field, Message: "must be a UUID"})
	}
	return &id, nil
}

// enumPtr projects an optional wire string onto a named enum pointer. huma has already validated the
// value against the field's enum tag, so no re-check is needed here.
func enumPtr[T ~string](s *string) *T {
	if s == nil {
		return nil
	}
	v := T(*s)
	return &v
}

// cursorString renders a Cursor for a page response, always present (null on the last page).
func cursorString(c string) *string {
	if c == "" {
		return nil
	}
	return &c
}

// notFound is the standard 404 for an addressed resource that does not exist.
func notFound(resource string) error {
	return humaerr.Fail(errs.ErrNotFound, "%s not found", resource)
}

// ptr returns a pointer to v. It is used to always populate a DTO field that the contract marks
// optional but that the server always knows (Huma renders such a field with omitempty, so a set
// pointer still appears while an absent one is omitted).
func ptr[T any](v T) *T { return &v }
