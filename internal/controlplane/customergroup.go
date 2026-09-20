package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// CustomerGroup is an organisational grouping of customers (control_plane.customer_groups, §6.17).
// It is a segmentation dimension and nothing more: it carries no balance, no quota and no
// configuration scope, it is never an inheritance level, and it never appears on the critical path.
// A group filter is resolved to customer_id IN (...) at read time, which is what keeps a breakdown
// accurate when a customer changes group.
type CustomerGroup struct {
	ID          uuid.UUID
	Name        string
	Description *string
	Status      CustomerGroupStatus
	CreatedBy   *uuid.UUID
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// NewCustomerGroup is the input to create a group. It has no status — creation is always active,
// the DDL default — and no CreatedBy: the operator identity arrives with real operator auth
// (step-310), as for sender IDs and routing scripts.
type NewCustomerGroup struct {
	Name        string
	Description *string
}

// CustomerGroupPatch is a partial update of a group. A nil field is left unchanged.
type CustomerGroupPatch struct {
	Name        *string
	Description *string
	Status      *CustomerGroupStatus
}

// CustomerGroupFilter narrows a group listing. There is no cursor: the listing is not paginated,
// because the contract returns a bare array.
type CustomerGroupFilter struct {
	Status *CustomerGroupStatus
}
