package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// CustomerGroup is an organisational grouping of customers (control_plane.customer_groups, §6.17):
// a segmentation dimension carrying no balance, no quota and no configuration scope, never an
// inheritance level, never on the critical path.
type CustomerGroup struct {
	ID          uuid.UUID
	Name        string
	Description *string
	Status      CustomerGroupStatus
	CreatedBy   *uuid.UUID
	CreatedAt   time.Time
	UpdatedAt   time.Time
	MemberCount int64
}

// NewCustomerGroup is the input to create a group. No status: creation is always active, the DDL
// default.
type NewCustomerGroup struct {
	Name        string
	Description *string
	CreatedBy   *uuid.UUID
}

// CustomerGroupPatch is a partial update of a group. A nil field is left unchanged.
type CustomerGroupPatch struct {
	Name        *string
	Description *string
	Status      *CustomerGroupStatus
}

// CustomerGroupFilter narrows a group listing. No cursor: the listing is not paginated.
type CustomerGroupFilter struct {
	Status *CustomerGroupStatus
}
