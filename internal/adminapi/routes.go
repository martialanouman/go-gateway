package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// routeTargetDTO is one weighted connector of a non-static route (contract schema RouteTarget).
type routeTargetDTO struct {
	ConnectorID string `json:"connector_id" format:"uuid"`
	Weight      *int   `json:"weight,omitempty" minimum:"1"`
	Priority    *int   `json:"priority,omitempty"`
}

// routeDTO is the wire form of a Route (contract schema Route).
type routeDTO struct {
	ID                   string           `json:"id" format:"uuid"`
	Name                 string           `json:"name"`
	Priority             int              `json:"priority"`
	MatchAccountID       *string          `json:"match_account_id,omitempty" format:"uuid" nullable:"true"`
	MatchCustomerID      *string          `json:"match_customer_id,omitempty" format:"uuid" nullable:"true"`
	MatchSenderPattern   *string          `json:"match_sender_pattern,omitempty" nullable:"true"`
	MatchDestPattern     *string          `json:"match_dest_pattern,omitempty" nullable:"true"`
	MatchContentPattern  *string          `json:"match_content_pattern,omitempty" nullable:"true"`
	DistributionStrategy string           `json:"distribution_strategy" enum:"static,round_robin,weighted,failover_priority,least_loaded,hash_based"`
	TargetConnectorID    *string          `json:"target_connector_id,omitempty" format:"uuid" nullable:"true"`
	FallbackRouteID      *string          `json:"fallback_route_id,omitempty" format:"uuid" nullable:"true"`
	Status               string           `json:"status" enum:"active,disabled"`
	Targets              []routeTargetDTO `json:"targets,omitempty"`
	CreatedAt            time.Time        `json:"created_at" format:"date-time"`
	UpdatedAt            time.Time        `json:"updated_at" format:"date-time"`
}

func toRouteDTO(r cp.Route) routeDTO {
	dto := routeDTO{
		ID:                   idString(r.ID),
		Name:                 r.Name,
		Priority:             r.Priority,
		MatchAccountID:       idPtr(r.MatchAccountID),
		MatchCustomerID:      idPtr(r.MatchCustomerID),
		MatchSenderPattern:   r.MatchSenderPattern,
		MatchDestPattern:     r.MatchDestPattern,
		MatchContentPattern:  r.MatchContentPattern,
		DistributionStrategy: string(r.DistributionStrategy),
		TargetConnectorID:    idPtr(r.TargetConnectorID),
		FallbackRouteID:      idPtr(r.FallbackRouteID),
		Status:               string(r.Status),
		CreatedAt:            r.CreatedAt,
		UpdatedAt:            r.UpdatedAt,
	}
	if len(r.Targets) > 0 {
		dto.Targets = make([]routeTargetDTO, 0, len(r.Targets))
		for _, t := range r.Targets {
			dto.Targets = append(dto.Targets, routeTargetDTO{
				ConnectorID: idString(t.ConnectorID),
				Weight:      ptr(t.Weight),
				Priority:    ptr(t.Priority),
			})
		}
	}
	return dto
}

type routeCreateBody struct {
	Name                 string           `json:"name"`
	Priority             *int             `json:"priority,omitempty"`
	MatchAccountID       *string          `json:"match_account_id,omitempty" format:"uuid" nullable:"true"`
	MatchCustomerID      *string          `json:"match_customer_id,omitempty" format:"uuid" nullable:"true"`
	MatchSenderPattern   *string          `json:"match_sender_pattern,omitempty" nullable:"true"`
	MatchDestPattern     *string          `json:"match_dest_pattern,omitempty" nullable:"true"`
	MatchContentPattern  *string          `json:"match_content_pattern,omitempty" nullable:"true"`
	DistributionStrategy string           `json:"distribution_strategy" enum:"static,round_robin,weighted,failover_priority,least_loaded,hash_based"`
	TargetConnectorID    *string          `json:"target_connector_id,omitempty" format:"uuid" nullable:"true"`
	FallbackRouteID      *string          `json:"fallback_route_id,omitempty" format:"uuid" nullable:"true"`
	Targets              []routeTargetDTO `json:"targets,omitempty"`
}

type routeUpdateBody struct {
	Name                 *string          `json:"name,omitempty"`
	Priority             *int             `json:"priority,omitempty"`
	MatchAccountID       *string          `json:"match_account_id,omitempty" format:"uuid" nullable:"true"`
	MatchCustomerID      *string          `json:"match_customer_id,omitempty" format:"uuid" nullable:"true"`
	MatchSenderPattern   *string          `json:"match_sender_pattern,omitempty" nullable:"true"`
	MatchDestPattern     *string          `json:"match_dest_pattern,omitempty" nullable:"true"`
	MatchContentPattern  *string          `json:"match_content_pattern,omitempty" nullable:"true"`
	DistributionStrategy *string          `json:"distribution_strategy,omitempty" enum:"static,round_robin,weighted,failover_priority,least_loaded,hash_based"`
	TargetConnectorID    *string          `json:"target_connector_id,omitempty" format:"uuid" nullable:"true"`
	FallbackRouteID      *string          `json:"fallback_route_id,omitempty" format:"uuid" nullable:"true"`
	Targets              []routeTargetDTO `json:"targets,omitempty"`
	Status               *string          `json:"status,omitempty" enum:"active,disabled"`
}

type routeHandlers struct {
	store RouteStore
}

func registerRoutes(api huma.API, store RouteStore) {
	h := &routeHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-routes", Method: http.MethodGet, Path: "/admin/routes",
		Summary: "List routes (ordered by priority)", Tags: []string{"Routes"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-route", Method: http.MethodPost, Path: "/admin/routes",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create a route", Tags: []string{"Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "get-route", Method: http.MethodGet, Path: "/admin/routes/{id}",
		Summary: "Get a route", Tags: []string{"Routes"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.get)

	register(api, huma.Operation{
		OperationID: "update-route", Method: http.MethodPatch, Path: "/admin/routes/{id}",
		Summary: "Update a route", Tags: []string{"Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-route", Method: http.MethodDelete, Path: "/admin/routes/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a route", Tags: []string{"Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)
}

type listRoutesOutput struct {
	Body []routeDTO
}

func (h *routeHandlers) list(ctx context.Context, _ *struct{}) (*listRoutesOutput, error) {
	routes, err := h.store.List(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listRoutesOutput{Body: make([]routeDTO, 0, len(routes))}
	for _, r := range routes {
		out.Body = append(out.Body, toRouteDTO(r))
	}
	return out, nil
}

type createRouteInput struct{ Body routeCreateBody }
type routeOutput struct{ Body routeDTO }

func (h *routeHandlers) create(ctx context.Context, in *createRouteInput) (*routeOutput, error) {
	strategy := cp.DistributionStrategy(in.Body.DistributionStrategy)
	targets, err := parseTargets(in.Body.Targets)
	if err != nil {
		return nil, err
	}
	targetConnectorID, err := parseIDPtr("target_connector_id", in.Body.TargetConnectorID)
	if err != nil {
		return nil, err
	}
	if err := validateStrategy(strategy, targetConnectorID, targets); err != nil {
		return nil, err
	}

	matchAccountID, err := parseIDPtr("match_account_id", in.Body.MatchAccountID)
	if err != nil {
		return nil, err
	}
	matchCustomerID, err := parseIDPtr("match_customer_id", in.Body.MatchCustomerID)
	if err != nil {
		return nil, err
	}
	fallbackRouteID, err := parseIDPtr("fallback_route_id", in.Body.FallbackRouteID)
	if err != nil {
		return nil, err
	}

	r, err := h.store.Create(ctx, cp.NewRoute{
		Name:                 in.Body.Name,
		Priority:             in.Body.Priority,
		MatchAccountID:       matchAccountID,
		MatchCustomerID:      matchCustomerID,
		MatchSenderPattern:   in.Body.MatchSenderPattern,
		MatchDestPattern:     in.Body.MatchDestPattern,
		MatchContentPattern:  in.Body.MatchContentPattern,
		DistributionStrategy: strategy,
		TargetConnectorID:    targetConnectorID,
		FallbackRouteID:      fallbackRouteID,
		Targets:              targets,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &routeOutput{Body: toRouteDTO(r)}, nil
}

type routeIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *routeHandlers) get(ctx context.Context, in *routeIDInput) (*routeOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("route")
	}
	r, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &routeOutput{Body: toRouteDTO(r)}, nil
}

type updateRouteInput struct {
	ID   string `path:"id" format:"uuid"`
	Body routeUpdateBody
}

func (h *routeHandlers) update(ctx context.Context, in *updateRouteInput) (*routeOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("route")
	}
	patch := cp.RoutePatch{
		Name:                 in.Body.Name,
		Priority:             in.Body.Priority,
		MatchSenderPattern:   in.Body.MatchSenderPattern,
		MatchDestPattern:     in.Body.MatchDestPattern,
		MatchContentPattern:  in.Body.MatchContentPattern,
		DistributionStrategy: enumPtr[cp.DistributionStrategy](in.Body.DistributionStrategy),
		Status:               enumPtr[cp.RouteStatus](in.Body.Status),
	}
	if patch.MatchAccountID, err = parseIDPtr("match_account_id", in.Body.MatchAccountID); err != nil {
		return nil, err
	}
	if patch.MatchCustomerID, err = parseIDPtr("match_customer_id", in.Body.MatchCustomerID); err != nil {
		return nil, err
	}
	if patch.TargetConnectorID, err = parseIDPtr("target_connector_id", in.Body.TargetConnectorID); err != nil {
		return nil, err
	}
	if patch.FallbackRouteID, err = parseIDPtr("fallback_route_id", in.Body.FallbackRouteID); err != nil {
		return nil, err
	}
	targetsProvided := in.Body.Targets != nil
	if targetsProvided {
		targets, terr := parseTargets(in.Body.Targets)
		if terr != nil {
			return nil, terr
		}
		patch.Targets = targets
	}

	// Validate the strategy invariant against the EFFECTIVE post-update state (patch value where
	// provided, current value otherwise), so a partial update cannot leave a route in a state
	// create-route would have rejected — e.g. PATCH {"targets":[]} on a weighted route. The
	// current route is read first for the effective values (and to 404 a missing route cleanly).
	// The read-then-write is not transactional; acceptable for a single-operator control plane.
	current, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	effStrategy := current.DistributionStrategy
	if patch.DistributionStrategy != nil {
		effStrategy = *patch.DistributionStrategy
	}
	effConnector := current.TargetConnectorID
	if patch.TargetConnectorID != nil {
		effConnector = patch.TargetConnectorID
	}
	effTargets := current.Targets
	if targetsProvided {
		effTargets = patch.Targets
	}
	if err := validateStrategy(effStrategy, effConnector, effTargets); err != nil {
		return nil, err
	}

	r, err := h.store.Update(ctx, id, patch)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &routeOutput{Body: toRouteDTO(r)}, nil
}

func (h *routeHandlers) delete(ctx context.Context, in *routeIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("route")
	}
	if err := h.store.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

// validateStrategy enforces the two rules the schema cannot express as a clean 422: a static route
// names exactly one connector, and a non-static route distributes over at least two targets.
func validateStrategy(strategy cp.DistributionStrategy, targetConnectorID *uuid.UUID, targets []cp.RouteTarget) error {
	if strategy == cp.DistributionStatic {
		if targetConnectorID == nil {
			return humaerr.FailValidation("a static route requires a target connector",
				humaerr.FieldError{Field: "target_connector_id", Message: "required when distribution_strategy is static"})
		}
		return nil
	}
	if len(targets) < 2 {
		return humaerr.FailValidation("a non-static route needs at least two targets",
			humaerr.FieldError{Field: "targets", Message: "at least 2 required when distribution_strategy is not static"})
	}
	return nil
}

// parseTargets converts the wire targets to domain targets, applying the schema defaults (weight 1,
// priority 0) so the DB CHECK on weight is never hit with a zero.
func parseTargets(in []routeTargetDTO) ([]cp.RouteTarget, error) {
	if in == nil {
		return nil, nil
	}
	out := make([]cp.RouteTarget, 0, len(in))
	for _, t := range in {
		id, err := uuid.Parse(t.ConnectorID)
		if err != nil {
			return nil, humaerr.FailValidation("invalid target connector_id",
				humaerr.FieldError{Field: "targets", Message: "each target connector_id must be a UUID"})
		}
		weight := 1
		if t.Weight != nil {
			weight = *t.Weight
		}
		priority := 0
		if t.Priority != nil {
			priority = *t.Priority
		}
		out = append(out, cp.RouteTarget{ConnectorID: id, Weight: weight, Priority: priority})
	}
	return out, nil
}
