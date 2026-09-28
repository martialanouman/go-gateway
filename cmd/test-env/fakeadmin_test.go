package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// fakeAdmin is an in-memory stand-in for the Admin API, wired exactly to the endpoints seed uses. Each
// wire struct here is deliberately separate from admin's own request/response types: it exists to catch
// a client that drifts from the real contract (api/openapi-admin.yaml), so it must not share code with
// the client it is checking.

type fakeCustomerRow struct {
	id, name string
	senders  []*fakeSenderRow
}

type fakeSenderRow struct {
	id, address, status string
}

type fakeAccountRow struct {
	id, customerID, name string
	credentials          []*fakeCredentialRow
}

type fakeCredentialRow struct {
	id, systemID, status string
	rotations            int
}

type fakeConnectorRow struct {
	id, name string
}

type fakeRouteRow struct {
	id, name, targetConnectorID string
}

type fakeAdmin struct {
	t   *testing.T
	srv *httptest.Server

	mu         sync.Mutex
	writes     int
	customers  []*fakeCustomerRow
	accounts   []*fakeAccountRow
	connectors []fakeConnectorRow
	routes     []fakeRouteRow
}

type fakePageMeta struct {
	NextCursor *string `json:"next_cursor"`
	HasMore    bool    `json:"has_more"`
}

type fakeCustomerCreate struct {
	Name string `json:"name"`
}

type fakeCustomerResp struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type fakeCustomerPage struct {
	fakePageMeta
	Data []fakeCustomerResp `json:"data"`
}

type fakeSenderIDCreate struct {
	Address string `json:"address"`
}

type fakeSenderIDUpdate struct {
	Status string `json:"status"`
}

type fakeSenderIDResp struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Status  string `json:"status"`
}

type fakeSmppAccountCreate struct {
	CustomerID       string `json:"customer_id"`
	Name             string `json:"name"`
	AllowedBindTypes string `json:"allowed_bind_types"`
	MaxSessions      int    `json:"max_sessions"`
}

type fakeSmppAccountResp struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	CustomerID string `json:"customer_id"`
}

type fakeCredentialCreate struct {
	Type     string `json:"type"`
	SystemID string `json:"system_id"`
}

type fakeCredentialResp struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	SystemID string `json:"system_id"`
	Status   string `json:"status"`
	Secret   string `json:"secret"`
}

type fakeConnectorCreate struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	BindType string `json:"bind_type"`
	SystemID string `json:"system_id"`
	Password string `json:"password"`
}

type fakeConnectorResp struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type fakeRouteCreate struct {
	Name                 string `json:"name"`
	Priority             int    `json:"priority"`
	DistributionStrategy string `json:"distribution_strategy"`
	TargetConnectorID    string `json:"target_connector_id"`
}

type fakeRouteResp struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	TargetConnectorID string `json:"target_connector_id"`
}

func newFakeAdmin(t *testing.T) *fakeAdmin {
	t.Helper()
	f := &fakeAdmin{t: t}
	f.customers = append(f.customers, &fakeCustomerRow{id: uuid.NewString(), name: "autre"})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/admin/customers", f.listCustomers)
	mux.HandleFunc("POST /v1/admin/customers", f.createCustomer)
	mux.HandleFunc("GET /v1/admin/customers/{id}/sender-ids", f.listSenderIDs)
	mux.HandleFunc("POST /v1/admin/customers/{id}/sender-ids", f.createSenderID)
	mux.HandleFunc("PATCH /v1/admin/customers/{id}/sender-ids/{sid}", f.updateSenderID)
	mux.HandleFunc("GET /v1/admin/customers/{id}/smpp-accounts", f.listAccounts)
	mux.HandleFunc("POST /v1/admin/smpp-accounts", f.createAccount)
	mux.HandleFunc("GET /v1/admin/smpp-accounts/{id}/credentials", f.listCredentials)
	mux.HandleFunc("POST /v1/admin/smpp-accounts/{id}/credentials", f.createCredential)
	mux.HandleFunc("POST /v1/admin/smpp-accounts/{id}/credentials/{cid}/rotate", f.rotateCredential)
	mux.HandleFunc("GET /v1/admin/connectors", f.listConnectors)
	mux.HandleFunc("POST /v1/admin/connectors", f.createConnector)
	mux.HandleFunc("GET /v1/admin/routes", f.listRoutes)
	mux.HandleFunc("POST /v1/admin/routes", f.createRoute)

	f.srv = httptest.NewServer(f.requireAuth(mux))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAdmin) admin() *admin {
	return &admin{base: f.srv.URL + "/v1/admin", token: "tok", hc: f.srv.Client()}
}

func (f *fakeAdmin) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, `{"code":"unauthorized","message":"jeton invalide"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeFakeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeFakeBody[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var body T
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		http.Error(w, `{"code":"validation_error","message":"corps invalide"}`, http.StatusUnprocessableEntity)
		return body, false
	}
	return body, true
}

func (f *fakeAdmin) findCustomer(id string) *fakeCustomerRow {
	for _, c := range f.customers {
		if c.id == id {
			return c
		}
	}
	return nil
}

func (f *fakeAdmin) findAccount(id string) *fakeAccountRow {
	for _, a := range f.accounts {
		if a.id == id {
			return a
		}
	}
	return nil
}

// listCustomers paginates by 1 record regardless of the requested limit, so a name search that isn't on
// the first page has to follow next_cursor to find it.
func (f *fakeAdmin) listCustomers(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	start := 0
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		n, err := strconv.Atoi(cursor)
		if err != nil {
			http.Error(w, `{"code":"validation_error","message":"curseur invalide"}`, http.StatusUnprocessableEntity)
			return
		}
		start = n
	}
	var page fakeCustomerPage
	if start < len(f.customers) {
		c := f.customers[start]
		page.Data = []fakeCustomerResp{{ID: c.id, Name: c.name}}
		if start+1 < len(f.customers) {
			page.HasMore = true
			next := strconv.Itoa(start + 1)
			page.NextCursor = &next
		}
	}
	writeFakeJSON(w, http.StatusOK, page)
}

func (f *fakeAdmin) createCustomer(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeCustomerCreate](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	c := &fakeCustomerRow{id: uuid.NewString(), name: body.Name}
	f.customers = append(f.customers, c)
	f.writes++
	f.mu.Unlock()
	writeFakeJSON(w, http.StatusCreated, fakeCustomerResp{ID: c.id, Name: c.name})
}

func (f *fakeAdmin) listSenderIDs(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.findCustomer(r.PathValue("id"))
	if c == nil {
		http.NotFound(w, r)
		return
	}
	out := make([]fakeSenderIDResp, 0, len(c.senders))
	for _, s := range c.senders {
		out = append(out, fakeSenderIDResp{ID: s.id, Address: s.address, Status: s.status})
	}
	writeFakeJSON(w, http.StatusOK, out)
}

func (f *fakeAdmin) createSenderID(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeSenderIDCreate](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.findCustomer(r.PathValue("id"))
	if c == nil {
		http.NotFound(w, r)
		return
	}
	s := &fakeSenderRow{id: uuid.NewString(), address: body.Address, status: "pending_carrier_approval"}
	c.senders = append(c.senders, s)
	f.writes++
	writeFakeJSON(w, http.StatusCreated, fakeSenderIDResp{ID: s.id, Address: s.address, Status: s.status})
}

func (f *fakeAdmin) updateSenderID(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeSenderIDUpdate](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.findCustomer(r.PathValue("id"))
	if c == nil {
		http.NotFound(w, r)
		return
	}
	for _, s := range c.senders {
		if s.id != r.PathValue("sid") {
			continue
		}
		s.status = body.Status
		f.writes++
		writeFakeJSON(w, http.StatusOK, fakeSenderIDResp{ID: s.id, Address: s.address, Status: s.status})
		return
	}
	http.NotFound(w, r)
}

func (f *fakeAdmin) listAccounts(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	customerID := r.PathValue("id")
	out := make([]fakeSmppAccountResp, 0, len(f.accounts))
	for _, a := range f.accounts {
		if a.customerID == customerID {
			out = append(out, fakeSmppAccountResp{ID: a.id, Name: a.name, CustomerID: a.customerID})
		}
	}
	writeFakeJSON(w, http.StatusOK, out)
}

func (f *fakeAdmin) createAccount(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeSmppAccountCreate](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	a := &fakeAccountRow{id: uuid.NewString(), customerID: body.CustomerID, name: body.Name}
	f.accounts = append(f.accounts, a)
	f.writes++
	f.mu.Unlock()
	writeFakeJSON(w, http.StatusCreated, fakeSmppAccountResp{ID: a.id, Name: a.name, CustomerID: a.customerID})
}

func (f *fakeAdmin) listCredentials(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.findAccount(r.PathValue("id"))
	if a == nil {
		http.NotFound(w, r)
		return
	}
	out := make([]fakeCredentialResp, 0, len(a.credentials))
	for _, c := range a.credentials {
		out = append(out, fakeCredentialResp{ID: c.id, Type: "smpp_bind", SystemID: c.systemID, Status: c.status})
	}
	writeFakeJSON(w, http.StatusOK, out)
}

func (f *fakeAdmin) createCredential(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeCredentialCreate](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.findAccount(r.PathValue("id"))
	if a == nil {
		http.NotFound(w, r)
		return
	}
	c := &fakeCredentialRow{id: uuid.NewString(), systemID: body.SystemID, status: "active"}
	a.credentials = append(a.credentials, c)
	f.writes++
	writeFakeJSON(w, http.StatusCreated, fakeCredentialResp{
		ID: c.id, Type: body.Type, SystemID: c.systemID, Status: c.status, Secret: uuid.NewString(),
	})
}

func (f *fakeAdmin) rotateCredential(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.findAccount(r.PathValue("id"))
	if a == nil {
		http.NotFound(w, r)
		return
	}
	for _, c := range a.credentials {
		if c.id != r.PathValue("cid") {
			continue
		}
		c.rotations++
		f.writes++
		writeFakeJSON(w, http.StatusOK, fakeCredentialResp{
			ID: c.id, Type: "smpp_bind", SystemID: c.systemID, Status: c.status, Secret: uuid.NewString(),
		})
		return
	}
	http.NotFound(w, r)
}

func (f *fakeAdmin) listConnectors(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeConnectorResp, 0, len(f.connectors))
	for _, c := range f.connectors {
		out = append(out, fakeConnectorResp{ID: c.id, Name: c.name})
	}
	writeFakeJSON(w, http.StatusOK, out)
}

func (f *fakeAdmin) createConnector(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeConnectorCreate](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	c := fakeConnectorRow{id: uuid.NewString(), name: body.Name}
	f.connectors = append(f.connectors, c)
	f.writes++
	f.mu.Unlock()
	writeFakeJSON(w, http.StatusCreated, fakeConnectorResp{ID: c.id, Name: c.name})
}

func (f *fakeAdmin) listRoutes(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeRouteResp, 0, len(f.routes))
	for _, rt := range f.routes {
		out = append(out, fakeRouteResp{ID: rt.id, Name: rt.name, TargetConnectorID: rt.targetConnectorID})
	}
	writeFakeJSON(w, http.StatusOK, out)
}

func (f *fakeAdmin) createRoute(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeRouteCreate](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	rt := fakeRouteRow{id: uuid.NewString(), name: body.Name, targetConnectorID: body.TargetConnectorID}
	f.routes = append(f.routes, rt)
	f.writes++
	f.mu.Unlock()
	writeFakeJSON(w, http.StatusCreated, fakeRouteResp{ID: rt.id, Name: rt.name, TargetConnectorID: rt.targetConnectorID})
}

func (f *fakeAdmin) accountID(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.accounts {
		if a.name == name {
			return a.id
		}
	}
	return ""
}

func (f *fakeAdmin) connectorID(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.connectors {
		if c.name == name {
			return c.id
		}
	}
	return ""
}

func (f *fakeAdmin) routeTarget(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.routes {
		if r.name == name {
			return r.targetConnectorID
		}
	}
	return ""
}

func (f *fakeAdmin) senderStatus(customerName, address string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.customers {
		if c.name != customerName {
			continue
		}
		for _, s := range c.senders {
			if s.address == address {
				return s.status
			}
		}
	}
	return ""
}

func (f *fakeAdmin) hasActiveCredential(accountName, systemID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.accounts {
		if a.name != accountName {
			continue
		}
		for _, c := range a.credentials {
			if c.systemID == systemID && c.status == "active" {
				return true
			}
		}
	}
	return false
}

func (f *fakeAdmin) preloadPendingSender(customerName, address string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var c *fakeCustomerRow
	for _, existing := range f.customers {
		if existing.name == customerName {
			c = existing
			break
		}
	}
	if c == nil {
		c = &fakeCustomerRow{id: uuid.NewString(), name: customerName}
		f.customers = append(f.customers, c)
	}
	c.senders = append(c.senders, &fakeSenderRow{id: uuid.NewString(), address: address, status: "pending_carrier_approval"})
}
