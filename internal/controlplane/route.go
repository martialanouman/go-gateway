package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// Route is a declarative routing rule (control_plane.routes). A static route names one connector
// in TargetConnectorID; a non-static route distributes over Targets (at least two).
type Route struct {
	ID                   uuid.UUID
	Name                 string
	Priority             int
	MatchAccountID       *uuid.UUID
	MatchCustomerID      *uuid.UUID
	MatchSenderPattern   *string
	MatchDestPattern     *string
	MatchContentPattern  *string
	DistributionStrategy DistributionStrategy
	TargetConnectorID    *uuid.UUID
	FallbackRouteID      *uuid.UUID
	Status               RouteStatus
	Targets              []RouteTarget
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// RouteTarget is one weighted connector of a non-static route (control_plane.route_targets).
type RouteTarget struct {
	ConnectorID uuid.UUID
	Weight      int
	Priority    int
}

// NewRoute is the input to create a route, with its targets. Priority defaults to the schema value
// when nil.
type NewRoute struct {
	Name                 string
	Priority             *int
	MatchAccountID       *uuid.UUID
	MatchCustomerID      *uuid.UUID
	MatchSenderPattern   *string
	MatchDestPattern     *string
	MatchContentPattern  *string
	DistributionStrategy DistributionStrategy
	TargetConnectorID    *uuid.UUID
	FallbackRouteID      *uuid.UUID
	Targets              []RouteTarget
}

// RoutePatch is a partial update of a route. A nil scalar field is left unchanged. Targets, when
// non-nil, replaces the whole target set (including an explicit empty set); nil leaves it as is.
type RoutePatch struct {
	Name                 *string
	Priority             *int
	MatchAccountID       *uuid.UUID
	MatchCustomerID      *uuid.UUID
	MatchSenderPattern   *string
	MatchDestPattern     *string
	MatchContentPattern  *string
	DistributionStrategy *DistributionStrategy
	TargetConnectorID    *uuid.UUID
	FallbackRouteID      *uuid.UUID
	Status               *RouteStatus
	Targets              []RouteTarget
}
