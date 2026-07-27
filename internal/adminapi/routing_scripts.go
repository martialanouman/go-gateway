package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/routing/script"
)

// routingScriptDTO is the wire form of a routing script (contract schema RoutingScript).
type routingScriptDTO struct {
	ID              string     `json:"id" format:"uuid"`
	Scope           string     `json:"scope" enum:"platform,customer,smpp_account"`
	ScopeID         *string    `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	Name            string     `json:"name"`
	Language        string     `json:"language" enum:"js,lua"`
	SourceCode      string     `json:"source_code"`
	Checksum        string     `json:"checksum"`
	Status          string     `json:"status" enum:"draft,active,disabled"`
	TimeoutMs       int        `json:"timeout_ms" minimum:"1" maximum:"20"`
	MaxInstructions *int64     `json:"max_instructions,omitempty" nullable:"true"`
	MaxMemoryKB     *int       `json:"max_memory_kb,omitempty" nullable:"true"`
	CreatedBy       *string    `json:"created_by,omitempty" format:"uuid" nullable:"true"`
	CreatedAt       time.Time  `json:"created_at" format:"date-time"`
	PublishedAt     *time.Time `json:"published_at,omitempty" format:"date-time" nullable:"true"`
}

func toRoutingScriptDTO(s script.Script) routingScriptDTO {
	return routingScriptDTO{
		ID:              idString(s.ID),
		Scope:           string(s.Scope),
		ScopeID:         idPtr(s.ScopeID),
		Name:            s.Name,
		Language:        string(s.Language),
		SourceCode:      s.Source,
		Checksum:        s.Checksum,
		Status:          string(s.Status),
		TimeoutMs:       s.TimeoutMs,
		MaxInstructions: s.MaxInstructions,
		MaxMemoryKB:     s.MaxMemoryKB,
		CreatedBy:       idPtr(s.CreatedBy),
		CreatedAt:       s.CreatedAt,
		PublishedAt:     s.PublishedAt,
	}
}

type routingScriptCreateBody struct {
	Scope           string  `json:"scope" enum:"platform,customer,smpp_account"`
	ScopeID         *string `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	Name            string  `json:"name"`
	Language        string  `json:"language" enum:"js,lua"`
	SourceCode      string  `json:"source_code"`
	TimeoutMs       int     `json:"timeout_ms,omitempty" minimum:"1" maximum:"20" default:"2"`
	MaxInstructions *int64  `json:"max_instructions,omitempty" nullable:"true"`
	MaxMemoryKB     *int    `json:"max_memory_kb,omitempty" nullable:"true"`
}

type routingScriptUpdateBody struct {
	Name            *string `json:"name,omitempty" nullable:"false"`
	SourceCode      *string `json:"source_code,omitempty" nullable:"false"`
	TimeoutMs       *int    `json:"timeout_ms,omitempty" minimum:"1" maximum:"20" nullable:"false"`
	MaxInstructions *int64  `json:"max_instructions,omitempty" nullable:"true"`
	MaxMemoryKB     *int    `json:"max_memory_kb,omitempty" nullable:"true"`
}

type scriptVersionDTO struct {
	Version     int        `json:"version"`
	Checksum    string     `json:"checksum"`
	Status      string     `json:"status" enum:"draft,active,disabled"`
	CreatedBy   *string    `json:"created_by,omitempty" format:"uuid" nullable:"true"`
	CreatedAt   time.Time  `json:"created_at" format:"date-time"`
	PublishedAt *time.Time `json:"published_at,omitempty" format:"date-time" nullable:"true"`
}

// RoutingScriptAdminStore is the persistence the routing-script handlers need (declared consumer-side).
// *postgres.RoutingScriptRepo satisfies it.
type RoutingScriptAdminStore interface {
	Create(ctx context.Context, s script.Script) (script.Script, error)
	Get(ctx context.Context, id uuid.UUID) (script.Script, bool, error)
	Update(ctx context.Context, id uuid.UUID, s script.Script) (script.Script, bool, error)
	Delete(ctx context.Context, id uuid.UUID) (bool, error)
	ListVersions(ctx context.Context, scope script.Scope, scopeID *uuid.UUID) ([]script.Script, error)
	List(ctx context.Context, after uuid.UUID, limit int) ([]script.Script, error)
	Assign(ctx context.Context, id uuid.UUID, scope script.Scope, scopeID *uuid.UUID) (script.Script, bool, error)
	Publish(ctx context.Context, id uuid.UUID) (script.Script, bool, error)
}

type routingScriptIDInput struct {
	ID string `path:"id" format:"uuid"`
}

type routingScriptHandlers struct {
	store RoutingScriptAdminStore
}

func registerRoutingScripts(api huma.API, store RoutingScriptAdminStore) {
	h := &routingScriptHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-routing-scripts", Method: http.MethodGet, Path: "/admin/routing-scripts",
		Summary: "List routing scripts", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-routing-script", Method: http.MethodPost, Path: "/admin/routing-scripts",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create a routing script (draft)", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "get-routing-script", Method: http.MethodGet, Path: "/admin/routing-scripts/{id}",
		Summary: "Get a routing script", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.get)

	register(api, huma.Operation{
		OperationID: "update-routing-script", Method: http.MethodPatch, Path: "/admin/routing-scripts/{id}",
		Summary: "Update a routing script (source/limits)", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-routing-script", Method: http.MethodDelete, Path: "/admin/routing-scripts/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a routing script", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)

	register(api, huma.Operation{
		OperationID: "list-routing-script-versions", Method: http.MethodGet, Path: "/admin/routing-scripts/{id}/versions",
		Summary: "List script versions", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.listVersions)

	register(api, huma.Operation{
		OperationID: "assign-routing-script", Method: http.MethodPatch, Path: "/admin/routing-scripts/{id}/assign",
		Summary: "Assign the script to a scope", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity},
	}, h.assign)

	register(api, huma.Operation{
		OperationID: "validate-routing-script", Method: http.MethodPost, Path: "/admin/routing-scripts/{id}/validate",
		Summary: "Static validation (syntax, limits)", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.validate)

	register(api, huma.Operation{
		OperationID: "test-routing-script", Method: http.MethodPost, Path: "/admin/routing-scripts/{id}/test",
		Summary: "Dry-run against a sample message", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.test)

	register(api, huma.Operation{
		OperationID: "publish-routing-script", Method: http.MethodPost, Path: "/admin/routing-scripts/{id}/publish",
		Summary: "Publish (activate)", Tags: []string{"Routing Scripts"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity},
	}, h.publish)
}

type assignRoutingScriptBody struct {
	Scope   string  `json:"scope" enum:"platform,customer,smpp_account"`
	ScopeID *string `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
}

type assignRoutingScriptInput struct {
	ID   string `path:"id" format:"uuid"`
	Body assignRoutingScriptBody
}

// assign reassigns a draft to a different scope. Only a draft can be reassigned (an active script's
// scope is immutable); the scope/scope_id invariant is re-validated.
func (h *routingScriptHandlers) assign(ctx context.Context, in *assignRoutingScriptInput) (*routingScriptOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("routing script")
	}
	current, found, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("routing script")
	}
	if current.Status != script.StatusDraft {
		return nil, humaerr.Fail(errs.ErrConflict, "only a draft routing script can be reassigned")
	}
	scopeID, err := parseIDPtr("scope_id", in.Body.ScopeID)
	if err != nil {
		return nil, err
	}
	if verr := validateScriptScope(script.Scope(in.Body.Scope), scopeID); verr != nil {
		return nil, verr
	}
	saved, found, err := h.store.Assign(ctx, id, script.Scope(in.Body.Scope), scopeID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("routing script")
	}
	return &routingScriptOutput{Body: toRoutingScriptDTO(saved)}, nil
}

type scriptValidateResultDTO struct {
	Valid    bool                 `json:"valid"`
	Checksum *string              `json:"checksum,omitempty" nullable:"true"`
	Errors   []scriptValidateItem `json:"errors,omitempty"`
}

type scriptValidateItem struct {
	Line    *int   `json:"line,omitempty" nullable:"true"`
	Message string `json:"message"`
}

type validateRoutingScriptOutput struct{ Body scriptValidateResultDTO }

// validate compiles the script in the same runtime the router uses. A compile failure is a valid=false
// result with the (operator-facing) error, not an HTTP error — the operator is diagnosing their own script.
func (h *routingScriptHandlers) validate(ctx context.Context, in *routingScriptIDInput) (*validateRoutingScriptOutput, error) {
	s, found, err := h.fetch(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound("routing script")
	}
	out := &validateRoutingScriptOutput{Body: scriptValidateResultDTO{Errors: []scriptValidateItem{}}}
	if _, cerr := script.NewResolver(s); cerr != nil {
		out.Body.Valid = false
		out.Body.Errors = append(out.Body.Errors, scriptValidateItem{Message: cerr.Error()})
		return out, nil
	}
	out.Body.Valid = true
	sum := s.Checksum
	out.Body.Checksum = &sum
	return out, nil
}

type scriptTestMessageBody struct {
	SourceAddr string  `json:"source_addr"`
	DestAddr   string  `json:"dest_addr"`
	Content    string  `json:"content"`
	AccountID  *string `json:"account_id,omitempty" format:"uuid" nullable:"true"`
	CustomerID *string `json:"customer_id,omitempty" format:"uuid" nullable:"true"`
}

type scriptTestRequestBody struct {
	Message scriptTestMessageBody `json:"message"`
}

type testRoutingScriptInput struct {
	ID   string `path:"id" format:"uuid"`
	Body scriptTestRequestBody
}

type scriptTestResultDTO struct {
	RouteID      *string  `json:"route_id,omitempty" format:"uuid" nullable:"true"`
	TookMs       float64  `json:"took_ms"`
	Instructions *int64   `json:"instructions,omitempty" nullable:"true"`
	TimedOut     bool     `json:"timed_out"`
	Error        *string  `json:"error,omitempty" nullable:"true"`
	Logs         []string `json:"logs,omitempty"`
}

type testRoutingScriptOutput struct{ Body scriptTestResultDTO }

// test dry-runs the script against a sample message in the SAME bounded sandbox as production (same
// timeout/limits), so a script too costly to run in prod fails here too. The sample content is never
// passed to the script (the routing runtime sees metadata only, invariant a) nor logged.
func (h *routingScriptHandlers) test(ctx context.Context, in *testRoutingScriptInput) (*testRoutingScriptOutput, error) {
	s, found, err := h.fetch(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound("routing script")
	}
	rt, cerr := script.NewResolver(s)
	out := &testRoutingScriptOutput{Body: scriptTestResultDTO{Logs: []string{}}}
	if cerr != nil {
		msg := cerr.Error()
		out.Body.Error = &msg
		return out, nil
	}

	// account_id/customer_id select WHICH script runs (scope) in production; a direct test runs THIS
	// script, so the runtime only needs the routing metadata it actually reads (from/to). No body.
	msg := script.Message{From: in.Body.Message.SourceAddr, To: in.Body.Message.DestAddr, Segments: 1}

	started := time.Now()
	routeID, rerr := rt.Resolve(ctx, msg)
	out.Body.TookMs = float64(time.Since(started).Microseconds()) / 1000.0

	switch {
	case rerr != nil:
		out.Body.TimedOut = script.Reason(rerr) == "timeout"
		msg := rerr.Error()
		out.Body.Error = &msg
	case routeID != nil:
		rid := routeID.String()
		out.Body.RouteID = &rid
	}
	return out, nil
}

// publish activates a draft (demoting any current active in the same scope, per the one-active index).
// A concurrent publish of two drafts for the same scope surfaces as 409.
func (h *routingScriptHandlers) publish(ctx context.Context, in *routingScriptIDInput) (*routingScriptOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("routing script")
	}
	// Compile-check before activating: an uncompilable active script is silently skipped by the router
	// (declarative fallback), so publishing one would disable routing with a success response. Reject it.
	current, found, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("routing script")
	}
	if _, cerr := script.NewResolver(current); cerr != nil {
		return nil, humaerr.FailValidation("script does not compile",
			humaerr.FieldError{Field: "source_code", Message: cerr.Error()})
	}
	saved, found, err := h.store.Publish(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("routing script")
	}
	return &routingScriptOutput{Body: toRoutingScriptDTO(saved)}, nil
}

type listRoutingScriptsInput struct {
	Scope   string `query:"scope" enum:"platform,customer,smpp_account" doc:"Filter by scope."`
	ScopeID string `query:"scopeId" format:"uuid" doc:"Filter by scope entity id."`
}

type listRoutingScriptsOutput struct{ Body []routingScriptDTO }

func (h *routingScriptHandlers) list(ctx context.Context, in *listRoutingScriptsInput) (*listRoutingScriptsOutput, error) {
	var scopeID *uuid.UUID
	if in.ScopeID != "" {
		id, err := uuid.Parse(in.ScopeID)
		if err != nil {
			return nil, humaerr.FailValidation("invalid scopeId", humaerr.FieldError{Field: "scopeId", Message: "must be a UUID"})
		}
		scopeID = &id
	}

	all, err := h.gatherAll(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listRoutingScriptsOutput{Body: make([]routingScriptDTO, 0, len(all))}
	for _, s := range all {
		if in.Scope != "" && string(s.Scope) != in.Scope {
			continue
		}
		if scopeID != nil && (s.ScopeID == nil || *s.ScopeID != *scopeID) {
			continue
		}
		out.Body = append(out.Body, toRoutingScriptDTO(s))
	}
	return out, nil
}

// gatherAll pages the full routing-script table. The table is operator-authored and small, so a full
// enumeration for the (unpaginated) admin list is fine.
func (h *routingScriptHandlers) gatherAll(ctx context.Context) ([]script.Script, error) {
	const page = 500
	var all []script.Script
	after := uuid.Nil
	for {
		batch, err := h.store.List(ctx, after, page)
		if err != nil {
			return nil, err
		}
		all = append(all, batch...)
		if len(batch) < page {
			break
		}
		after = batch[len(batch)-1].ID
	}
	return all, nil
}

type createRoutingScriptInput struct{ Body routingScriptCreateBody }
type routingScriptOutput struct{ Body routingScriptDTO }

func (h *routingScriptHandlers) create(ctx context.Context, in *createRoutingScriptInput) (*routingScriptOutput, error) {
	scopeID, err := parseIDPtr("scope_id", in.Body.ScopeID)
	if err != nil {
		return nil, err
	}
	if verr := validateScriptScope(script.Scope(in.Body.Scope), scopeID); verr != nil {
		return nil, verr
	}
	timeout := in.Body.TimeoutMs
	if timeout == 0 {
		timeout = 2
	}
	saved, err := h.store.Create(ctx, script.Script{
		Scope:           script.Scope(in.Body.Scope),
		ScopeID:         scopeID,
		Name:            in.Body.Name,
		Language:        script.Language(in.Body.Language),
		Source:          in.Body.SourceCode,
		Checksum:        script.Checksum(in.Body.SourceCode),
		TimeoutMs:       timeout,
		MaxInstructions: in.Body.MaxInstructions,
		MaxMemoryKB:     in.Body.MaxMemoryKB,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &routingScriptOutput{Body: toRoutingScriptDTO(saved)}, nil
}

func (h *routingScriptHandlers) get(ctx context.Context, in *routingScriptIDInput) (*routingScriptOutput, error) {
	s, found, err := h.fetch(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound("routing script")
	}
	return &routingScriptOutput{Body: toRoutingScriptDTO(s)}, nil
}

type updateRoutingScriptInput struct {
	ID   string `path:"id" format:"uuid"`
	Body routingScriptUpdateBody
}

func (h *routingScriptHandlers) update(ctx context.Context, in *updateRoutingScriptInput) (*routingScriptOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("routing script")
	}
	current, found, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("routing script")
	}
	// Only a draft is editable: an active/disabled script is immutable, so a live routing change always
	// goes through create-a-new-draft → publish (which demotes the old active). Editing an active script
	// in place would bypass validate/test/publish and mutate live routing with no new version.
	if current.Status != script.StatusDraft {
		return nil, humaerr.Fail(errs.ErrConflict, "only a draft routing script can be edited; create a new draft and publish it")
	}
	if in.Body.Name != nil {
		current.Name = *in.Body.Name
	}
	if in.Body.SourceCode != nil {
		current.Source = *in.Body.SourceCode
		current.Checksum = script.Checksum(*in.Body.SourceCode) // recompute on a source change
	}
	if in.Body.TimeoutMs != nil {
		current.TimeoutMs = *in.Body.TimeoutMs
	}
	if in.Body.MaxInstructions != nil {
		current.MaxInstructions = in.Body.MaxInstructions
	}
	if in.Body.MaxMemoryKB != nil {
		current.MaxMemoryKB = in.Body.MaxMemoryKB
	}

	saved, found, err := h.store.Update(ctx, id, current)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("routing script")
	}
	return &routingScriptOutput{Body: toRoutingScriptDTO(saved)}, nil
}

func (h *routingScriptHandlers) delete(ctx context.Context, in *routingScriptIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("routing script")
	}
	found, err := h.store.Delete(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("routing script")
	}
	return &deleteOutput{}, nil
}

type listVersionsOutput struct{ Body []scriptVersionDTO }

func (h *routingScriptHandlers) listVersions(ctx context.Context, in *routingScriptIDInput) (*listVersionsOutput, error) {
	s, found, err := h.fetch(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound("routing script")
	}
	versions, err := h.store.ListVersions(ctx, s.Scope, s.ScopeID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listVersionsOutput{Body: make([]scriptVersionDTO, 0, len(versions))}
	// ListVersions is newest-first; number them so the oldest is version 1 and the newest is the highest.
	for i, v := range versions {
		out.Body = append(out.Body, scriptVersionDTO{
			Version:     len(versions) - i,
			Checksum:    v.Checksum,
			Status:      string(v.Status),
			CreatedBy:   idPtr(v.CreatedBy),
			CreatedAt:   v.CreatedAt,
			PublishedAt: v.PublishedAt,
		})
	}
	return out, nil
}

// fetch parses the id and loads the script.
func (h *routingScriptHandlers) fetch(ctx context.Context, rawID string) (script.Script, bool, error) {
	id, err := uuid.Parse(rawID)
	if err != nil {
		return script.Script{}, false, nil
	}
	s, found, err := h.store.Get(ctx, id)
	if err != nil {
		return script.Script{}, false, humaerr.FromError(err)
	}
	return s, found, nil
}

// validateScriptScope enforces the schema invariant: platform has no scope_id; every other scope
// requires one.
func validateScriptScope(scope script.Scope, scopeID *uuid.UUID) error {
	if scope == script.ScopePlatform && scopeID != nil {
		return humaerr.FailValidation("invalid scope", humaerr.FieldError{Field: "scope_id", Message: "must be null for the platform scope"})
	}
	if scope != script.ScopePlatform && scopeID == nil {
		return humaerr.FailValidation("invalid scope", humaerr.FieldError{Field: "scope_id", Message: "required for a non-platform scope"})
	}
	return nil
}
