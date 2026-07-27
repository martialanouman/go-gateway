// Package script models routing scripts (schema §12): operator-authored JS/Lua that resolves a
// message to a route, evaluated between the L0 exact short-cut and the declarative resolver (§6.1).
// This file holds the domain types and lifecycle; persistence lives in internal/storage/postgres and
// the bounded runtimes (goja/gopher-lua) in this package (step-108/109).
package script

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

// Scope is where a routing script applies (control_plane.routing_scripts.scope). Resolution walks
// account → customer → platform, first active wins (like anti-spam / opt-out).
type Scope string

// The routing-script scopes. scope_id is the matching entity id; nil only for platform.
const (
	ScopePlatform Scope = "platform"
	ScopeCustomer Scope = "customer"
	ScopeAccount  Scope = "smpp_account"
)

// Language is the script runtime (control_plane.routing_scripts.language).
type Language string

// The supported script languages, each with a bounded runtime (step-108/109).
const (
	LanguageJS  Language = "js"
	LanguageLua Language = "lua"
)

// Status is a script's lifecycle state. At most one script may be active per (scope, scope_id) — the
// schema enforces it with a partial unique index, so publish is a transactional demote-then-promote.
type Status string

// The routing-script lifecycle states.
const (
	StatusDraft    Status = "draft"
	StatusActive   Status = "active"
	StatusDisabled Status = "disabled"
)

// Script is one routing script version. Checksum is the SHA-256 of Source, computed on write, so a
// compiled program can be cached by content. TimeoutMs is the wall-clock cap (schema: 1..20ms);
// MaxInstructions is the primary deterministic guard (step-108); MaxMemoryKB is a best-effort net.
type Script struct {
	ID              uuid.UUID
	Scope           Scope
	ScopeID         *uuid.UUID
	Name            string
	Language        Language
	Source          string
	Checksum        string
	Status          Status
	TimeoutMs       int
	MaxInstructions *int64
	MaxMemoryKB     *int
	CreatedBy       *uuid.UUID
	CreatedAt       time.Time
	PublishedAt     *time.Time
}

// Checksum is the SHA-256 hex digest of a script's source, the content key used to cache a compiled
// program across executions (step-108/109) and to detect a changed source on update.
func Checksum(source string) string {
	sum := sha256.Sum256([]byte(source))
	return hex.EncodeToString(sum[:])
}
