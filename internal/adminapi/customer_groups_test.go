package adminapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// newGroupAPI wires the Admin API with both stores the group surface needs: list-group-customers
// reads the group to honour its 404, then the customers to fill the page.
func newGroupAPI(t *testing.T, groups adminapi.CustomerGroupStore, customers adminapi.CustomerStore) http.Handler {
	t.Helper()
	return newTestAPIWith(t, adminapi.Deps{CustomerGroups: groups, Customers: customers})
}

func TestCreateCustomerGroupReturns201WithTheCreatedGroup(t *testing.T) {
	api := newGroupAPI(t, newFakeCustomerGroupStore(), newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customer-groups",
		`{"name":"Carriers","description":"West Africa"}`))

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["name"] != "Carriers" {
		t.Errorf("name = %v, want Carriers", got["name"])
	}
	// active comes from the double, not from the schema default — what this catches is a DTO that
	// drops Status. The default itself is proved in the repository's round-trip.
	if got["status"] != "active" {
		t.Errorf("status = %v, want active", got["status"])
	}
	if got["description"] != "West Africa" {
		t.Errorf("description = %v, want West Africa", got["description"])
	}
}

// TestCreateCustomerGroupConflictBecomes409: customer_groups.name is UNIQUE, and 409 is the code the
// contract declares for it.
func TestCreateCustomerGroupConflictBecomes409(t *testing.T) {
	groups := newFakeCustomerGroupStore()
	groups.createErr = errs.ErrConflict
	api := newGroupAPI(t, groups, newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/customer-groups", `{"name":"Taken"}`))

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "conflict" {
		t.Errorf("code = %v, want conflict", got["code"])
	}
}

// TestListCustomerGroupsReturnsABareArray: the contract returns array<CustomerGroup>, not a paginated
// envelope. Decoding into a slice is the assertion — a page object would fail to unmarshal.
func TestListCustomerGroupsReturnsABareArray(t *testing.T) {
	groups := newFakeCustomerGroupStore()
	groups.seed(cp.CustomerGroup{ID: uuid.New(), Name: "Alpha", Status: cp.CustomerGroupActive})
	groups.seed(cp.CustomerGroup{ID: uuid.New(), Name: "Beta", Status: cp.CustomerGroupArchived})
	api := newGroupAPI(t, groups, newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customer-groups", ""))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got []map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("body is not a bare array: %v; body=%s", err, w.Body)
	}
	if len(got) != 2 {
		t.Errorf("returned %d groups, want 2", len(got))
	}
}

// TestListCustomerGroupsFiltersByStatus: ?status= reaches the store as a filter rather than being
// dropped on the floor.
func TestListCustomerGroupsFiltersByStatus(t *testing.T) {
	groups := newFakeCustomerGroupStore()
	groups.seed(cp.CustomerGroup{ID: uuid.New(), Name: "Alpha", Status: cp.CustomerGroupActive})
	groups.seed(cp.CustomerGroup{ID: uuid.New(), Name: "Beta", Status: cp.CustomerGroupArchived})
	api := newGroupAPI(t, groups, newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customer-groups?status=archived", ""))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got) != 1 || got[0]["name"] != "Beta" {
		t.Errorf("got %v, want only the archived group Beta", got)
	}
}

// TestListCustomerGroupsRejectsAnUnknownStatus: the shared Status parameter is a free-form string in
// the contract, so the handler is what holds the enum — and 422 is the code declared for it.
func TestListCustomerGroupsRejectsAnUnknownStatus(t *testing.T) {
	api := newGroupAPI(t, newFakeCustomerGroupStore(), newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customer-groups?status=retired", ""))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

func TestGetMissingCustomerGroupIs404(t *testing.T) {
	api := newGroupAPI(t, newFakeCustomerGroupStore(), newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet,
		"/v1/admin/customer-groups/00000000-0000-7000-8000-000000000000", ""))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["code"] != "not_found" {
		t.Errorf("code = %v, want not_found", got["code"])
	}
}

func TestUpdateCustomerGroupArchivesIt(t *testing.T) {
	groups := newFakeCustomerGroupStore()
	id := uuid.New()
	groups.seed(cp.CustomerGroup{ID: id, Name: "Alpha", Status: cp.CustomerGroupActive})
	api := newGroupAPI(t, groups, newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/customer-groups/"+id.String(),
		`{"status":"archived"}`))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "archived" {
		t.Errorf("status = %v, want archived", got["status"])
	}
}

func TestDeleteCustomerGroupReturns204(t *testing.T) {
	groups := newFakeCustomerGroupStore()
	id := uuid.New()
	groups.seed(cp.CustomerGroup{ID: id, Name: "Doomed", Status: cp.CustomerGroupActive})
	api := newGroupAPI(t, groups, newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/customer-groups/"+id.String(), ""))

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", w.Code, w.Body)
	}

	// The 204 alone says nothing about what the list still shows: a store that forgets the id in
	// one index and keeps it in another answers 204 and then serves a nameless ghost.
	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customer-groups", ""))
	var got []map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got) != 0 {
		t.Errorf("list returned %v after the delete, want empty", got)
	}
}

// TestCustomerGroupNameCannotBeEmpty: name is the group's only human label, and it is UNIQUE. An
// empty one blanks the row in every operator picker and squats the unique index for good — so both
// bodies declare minLength 1, and the pointer on PATCH makes "absent" the only way to leave the
// name alone.
func TestCustomerGroupNameCannotBeEmpty(t *testing.T) {
	groups := newFakeCustomerGroupStore()
	id := uuid.New()
	groups.seed(cp.CustomerGroup{ID: id, Name: "Alpha", Status: cp.CustomerGroupActive})
	api := newGroupAPI(t, groups, newFakeCustomerStore())

	for _, tc := range []struct{ name, method, path string }{
		{"create", http.MethodPost, "/v1/admin/customer-groups"},
		{"update", http.MethodPatch, "/v1/admin/customer-groups/" + id.String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			api.ServeHTTP(w, authed(t, tc.method, tc.path, `{"name":""}`))

			if w.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
			}
		})
	}

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customer-groups/"+id.String(), ""))
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["name"] != "Alpha" {
		t.Errorf("name = %v, want Alpha — the rejected PATCH still reached the store", got["name"])
	}
}

func TestDeleteMissingCustomerGroupIs404(t *testing.T) {
	api := newGroupAPI(t, newFakeCustomerGroupStore(), newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete,
		"/v1/admin/customer-groups/00000000-0000-7000-8000-000000000000", ""))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 — a silent 204 would hide the missing group", w.Code)
	}
}

// TestListGroupCustomersUnknownGroupIs404 is why the handler reads the group at all: without it an
// unknown group would answer 200 with an empty page, and the contract declares 404.
func TestListGroupCustomersUnknownGroupIs404(t *testing.T) {
	api := newGroupAPI(t, newFakeCustomerGroupStore(), newFakeCustomerStore())

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet,
		"/v1/admin/customer-groups/00000000-0000-7000-8000-000000000000/customers", ""))

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body)
	}
}

// TestListGroupCustomersReturnsOnlyItsMembers: the fixture holds a customer OUTSIDE the group on
// purpose — a handler that ignored the filter and returned everything would pass otherwise.
func TestListGroupCustomersReturnsOnlyItsMembers(t *testing.T) {
	groups := newFakeCustomerGroupStore()
	groupID := uuid.New()
	groups.seed(cp.CustomerGroup{ID: groupID, Name: "Members", Status: cp.CustomerGroupActive})

	customers := newFakeCustomerStore()
	if _, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Inside", GroupID: &groupID}); err != nil {
		t.Fatalf("create inside: %v", err)
	}
	if _, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Outside"}); err != nil {
		t.Fatalf("create outside: %v", err)
	}
	api := newGroupAPI(t, groups, customers)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet,
		"/v1/admin/customer-groups/"+groupID.String()+"/customers", ""))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got struct {
		Data []map[string]any `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got.Data) != 1 {
		t.Fatalf("returned %d customers, want only the one in the group; body=%s", len(got.Data), w.Body)
	}
	if got.Data[0]["name"] != "Inside" {
		t.Errorf("name = %v, want Inside", got.Data[0]["name"])
	}
}

// TestSetCustomerGroupAttachesThenDetaches walks the whole point of the endpoint: membership can
// change after creation, and null clears it.
func TestSetCustomerGroupAttachesThenDetaches(t *testing.T) {
	groupID := uuid.New()
	customers := newFakeCustomerStore()
	customer, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Mover"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	api := newGroupAPI(t, newFakeCustomerGroupStore(), customers)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/customers/"+customer.ID.String()+"/group",
		`{"group_id":"`+groupID.String()+`"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("attach status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var attached map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &attached)
	if attached["group_id"] != groupID.String() {
		t.Errorf("group_id = %v, want %s", attached["group_id"], groupID)
	}

	w = httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/customers/"+customer.ID.String()+"/group",
		`{"group_id":null}`))
	if w.Code != http.StatusOK {
		t.Fatalf("detach status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var detached map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &detached)
	if detached["group_id"] != nil {
		t.Errorf("group_id = %v after clearing, want null", detached["group_id"])
	}
	if detached["name"] != "Mover" {
		t.Errorf("name = %v — detaching must not delete or replace the customer", detached["name"])
	}
}

// TestSetCustomerGroupUnknownGroupIs422 proves only that the handler passes a store validation
// error through as 422 rather than swallowing it. That the FK is what produces that error is a
// different claim, proved against a real database by TestSetCustomerGroupUnknownGroupIsValidation.
func TestSetCustomerGroupUnknownGroupIs422(t *testing.T) {
	customers := newFakeCustomerStore()
	customers.setGroupErr = errs.ErrValidation
	customer, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Mover"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	api := newGroupAPI(t, newFakeCustomerGroupStore(), customers)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/customers/"+customer.ID.String()+"/group",
		`{"group_id":"`+uuid.New().String()+`"}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

// TestSetCustomerGroupRequiresTheField: group_id is required in the contract, so an empty body is a
// validation failure and not "leave it as it is" — the absent/null distinction this endpoint avoids
// having to make.
func TestSetCustomerGroupRequiresTheField(t *testing.T) {
	customers := newFakeCustomerStore()
	customer, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Mover"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	api := newGroupAPI(t, newFakeCustomerGroupStore(), customers)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/customers/"+customer.ID.String()+"/group", `{}`))

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", w.Code, w.Body)
	}
}

// TestGroupFilterOfListCustomersIsWiredThrough and its accounts twin are the end-to-end half of what
// the task file asks for: the repository proves the SQL of ?groupId=, these two prove that the
// endpoint hands the parameter to it. Without them, deleting `filter.GroupID = &id` from either
// handler leaves the whole suite green — the contract test compares operationIds, codes and schemas,
// never parameters.
//
// The fixture is asymmetric on purpose: one customer in the group, one outside.
func TestGroupFilterOfListCustomersIsWiredThrough(t *testing.T) {
	groupID := uuid.New()
	customers := newFakeCustomerStore()
	if _, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Inside", GroupID: &groupID}); err != nil {
		t.Fatalf("create inside: %v", err)
	}
	if _, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Outside"}); err != nil {
		t.Fatalf("create outside: %v", err)
	}
	api := newTestAPI(t, customers)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/customers?groupId="+groupID.String(), ""))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got struct {
		Data []map[string]any `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got.Data) != 1 {
		t.Fatalf("returned %d customers, want only the group member; body=%s", len(got.Data), w.Body)
	}
	if got.Data[0]["name"] != "Inside" {
		t.Errorf("name = %v, want Inside", got.Data[0]["name"])
	}
}

func TestGroupFilterOfListAccountsIsWiredThrough(t *testing.T) {
	groupID := uuid.New()
	customers := newFakeCustomerStore()
	inside, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Inside", GroupID: &groupID})
	if err != nil {
		t.Fatalf("create inside: %v", err)
	}
	outside, err := customers.Create(t.Context(), cp.NewCustomer{Name: "Outside"})
	if err != nil {
		t.Fatalf("create outside: %v", err)
	}

	accounts := newFakeAccountStore()
	accounts.customers = customers
	if _, err := accounts.Create(t.Context(), cp.NewAccount{CustomerID: inside.ID, Name: "in-app"}); err != nil {
		t.Fatalf("create inside account: %v", err)
	}
	if _, err := accounts.Create(t.Context(), cp.NewAccount{CustomerID: outside.ID, Name: "out-app"}); err != nil {
		t.Fatalf("create outside account: %v", err)
	}
	api := newTestAPIWith(t, adminapi.Deps{Customers: customers, Accounts: accounts})

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodGet, "/v1/admin/smpp-accounts?groupId="+groupID.String(), ""))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body)
	}
	var got struct {
		Data []map[string]any `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if len(got.Data) != 1 {
		t.Fatalf("returned %d accounts, want only the one owned by the group member; body=%s",
			len(got.Data), w.Body)
	}
	if got.Data[0]["name"] != "in-app" {
		t.Errorf("name = %v, want in-app", got.Data[0]["name"])
	}
}
