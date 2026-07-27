package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
)

// exactRouteDTO is the wire form of an exact route (contract schema ExactRoute).
type exactRouteDTO struct {
	MSISDN     string     `json:"msisdn"`
	TargetType string     `json:"target_type" enum:"connector,route"`
	TargetID   string     `json:"target_id" format:"uuid"`
	Source     string     `json:"source" enum:"mnp_import,manual,carrier_feed"`
	ImportedAt *time.Time `json:"imported_at,omitempty" format:"date-time" nullable:"true"`
	UpdatedAt  time.Time  `json:"updated_at" format:"date-time"`
}

func toExactRouteDTO(r exact.Route) exactRouteDTO {
	return exactRouteDTO{
		MSISDN:     r.MSISDN,
		TargetType: string(r.Target.Type),
		TargetID:   r.Target.ID.String(),
		Source:     string(r.Source),
		ImportedAt: r.ImportedAt,
		UpdatedAt:  r.UpdatedAt,
	}
}

type exactRouteCreateBody struct {
	MSISDN     string `json:"msisdn"`
	TargetType string `json:"target_type" enum:"connector,route"`
	TargetID   string `json:"target_id" format:"uuid"`
	Source     string `json:"source,omitempty" enum:"mnp_import,manual,carrier_feed" default:"manual"`
}

type exactRouteUpdateBody struct {
	TargetType *string `json:"target_type,omitempty" enum:"connector,route" nullable:"false"`
	TargetID   *string `json:"target_id,omitempty" format:"uuid" nullable:"false"`
	Source     *string `json:"source,omitempty" enum:"mnp_import,manual,carrier_feed" nullable:"false"`
}

type exactRoutePage struct {
	PageMeta
	Data []exactRouteDTO `json:"data"`
}

// ExactRouteAdminStore is the persistence the exact-route handlers need (declared consumer-side).
// *postgres.ExactRouteRepo satisfies it. List is keyset-paginated by msisdn (the primary key), so the
// cursor is simply the last msisdn of the previous page.
type ExactRouteAdminStore interface {
	Get(ctx context.Context, msisdn string) (exact.Route, bool, error)
	List(ctx context.Context, after string, limit int) ([]exact.Route, error)
	Upsert(ctx context.Context, route exact.Route) (exact.Route, error)
	Delete(ctx context.Context, msisdn string) (bool, error)
}

type exactRouteHandlers struct {
	store ExactRouteAdminStore
}

func registerExactRoutes(api huma.API, store ExactRouteAdminStore) {
	h := &exactRouteHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-exact-routes", Method: http.MethodGet, Path: "/admin/exact-routes",
		Summary: "List exact routes", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-exact-route", Method: http.MethodPost, Path: "/admin/exact-routes",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create/upsert an exact route (MSISDN -> connector|route)", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "lookup-exact-route", Method: http.MethodGet, Path: "/admin/exact-routes/lookup",
		Summary: "Look up the exact route for an MSISDN", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.lookup)

	register(api, huma.Operation{
		OperationID: "update-exact-route", Method: http.MethodPatch, Path: "/admin/exact-routes/{msisdn}",
		Summary: "Update an exact route", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-exact-route", Method: http.MethodDelete, Path: "/admin/exact-routes/{msisdn}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete an exact route", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)
}

type listExactRoutesInput struct {
	Cursor string `query:"cursor" doc:"Last msisdn of the previous page (the next_cursor returned by the prior page)."`
	Limit  int    `query:"limit" minimum:"1" maximum:"500" default:"50" doc:"Page size."`
}

type listExactRoutesOutput struct{ Body exactRoutePage }

func (h *exactRouteHandlers) list(ctx context.Context, in *listExactRoutesInput) (*listExactRoutesOutput, error) {
	// Fetch one extra row to decide has_more without a second query. The cursor is the last msisdn, so
	// the keyset resumes strictly after it (repo List uses `msisdn > after`).
	rows, err := h.store.List(ctx, in.Cursor, in.Limit+1)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listExactRoutesOutput{}
	hasMore := len(rows) > in.Limit
	if hasMore {
		rows = rows[:in.Limit]
	}
	out.Body.HasMore = hasMore
	if hasMore && len(rows) > 0 {
		out.Body.NextCursor = cursorString(rows[len(rows)-1].MSISDN)
	}
	out.Body.Data = make([]exactRouteDTO, 0, len(rows))
	for _, r := range rows {
		out.Body.Data = append(out.Body.Data, toExactRouteDTO(r))
	}
	return out, nil
}

type createExactRouteInput struct{ Body exactRouteCreateBody }
type exactRouteOutput struct{ Body exactRouteDTO }

// create upserts a single exact route. It is a full replace keyed by msisdn: re-creating an existing
// number overwrites its source and clears imported_at (a manual override converts an imported row).
// Use PATCH (update) to change a target while preserving the import provenance.
func (h *exactRouteHandlers) create(ctx context.Context, in *createExactRouteInput) (*exactRouteOutput, error) {
	msisdn, err := normalizeMSISDN(in.Body.MSISDN)
	if err != nil {
		return nil, err
	}
	target, err := parseTarget(in.Body.TargetType, in.Body.TargetID)
	if err != nil {
		return nil, err
	}
	source := exact.SourceManual
	if in.Body.Source != "" {
		source = exact.Source(in.Body.Source)
	}
	saved, err := h.store.Upsert(ctx, exact.Route{MSISDN: msisdn, Target: target, Source: source})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &exactRouteOutput{Body: toExactRouteDTO(saved)}, nil
}

type msisdnPathInput struct {
	MSISDN string `path:"msisdn"`
}

type updateExactRouteInput struct {
	MSISDN string `path:"msisdn"`
	Body   exactRouteUpdateBody
}

func (h *exactRouteHandlers) update(ctx context.Context, in *updateExactRouteInput) (*exactRouteOutput, error) {
	msisdn, err := normalizeMSISDN(in.MSISDN)
	if err != nil {
		return nil, err
	}
	current, found, err := h.store.Get(ctx, msisdn)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("exact route")
	}

	// target_type and target_id are a unit (a connector id is meaningless as a route id): change both
	// together or neither, so a PATCH cannot leave the row pointing a target id at the wrong kind.
	if (in.Body.TargetType == nil) != (in.Body.TargetID == nil) {
		return nil, humaerr.FailValidation("incomplete target",
			humaerr.FieldError{Field: "target_id", Message: "target_type and target_id must be set together"})
	}
	if in.Body.TargetType != nil {
		target, terr := parseTarget(*in.Body.TargetType, *in.Body.TargetID)
		if terr != nil {
			return nil, terr
		}
		current.Target = target
	}
	if in.Body.Source != nil {
		current.Source = exact.Source(*in.Body.Source)
	}

	saved, err := h.store.Upsert(ctx, current) // preserves ImportedAt, refreshes updated_at
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &exactRouteOutput{Body: toExactRouteDTO(saved)}, nil
}

func (h *exactRouteHandlers) delete(ctx context.Context, in *msisdnPathInput) (*deleteOutput, error) {
	msisdn, err := normalizeMSISDN(in.MSISDN)
	if err != nil {
		return nil, notFound("exact route")
	}
	found, err := h.store.Delete(ctx, msisdn)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("exact route")
	}
	return &deleteOutput{}, nil
}

type lookupExactRouteInput struct {
	MSISDN string `query:"msisdn" required:"true" doc:"MSISDN to resolve (E.164)."`
}

func (h *exactRouteHandlers) lookup(ctx context.Context, in *lookupExactRouteInput) (*exactRouteOutput, error) {
	msisdn, err := normalizeMSISDN(in.MSISDN)
	if err != nil {
		return nil, err
	}
	r, found, err := h.store.Get(ctx, msisdn)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("exact route")
	}
	return &exactRouteOutput{Body: toExactRouteDTO(r)}, nil
}

// parseTarget validates a target_type/target_id pair into an exact.Target. The enum tag already bounds
// target_type at the schema; this guards the uuid and keeps the domain type the single constructor.
func parseTarget(targetType, targetID string) (exact.Target, error) {
	id, err := uuid.Parse(targetID)
	if err != nil {
		return exact.Target{}, humaerr.FailValidation("invalid target",
			humaerr.FieldError{Field: "target_id", Message: "must be a UUID"})
	}
	return exact.Target{Type: exact.TargetType(targetType), ID: id}, nil
}
