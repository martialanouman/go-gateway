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
	id, typ, systemID, status, secret string
	rotations                         int
}

type fakeConnectorRow struct {
	id, name     string
	bindPoolSize int
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

	antispamRules []fakeAntispamRule
	billing       map[string]fakeBilling
	exactRoutes   []fakeExactRouteCreate

	lastAccount    fakeSmppAccountCreate
	lastCredential fakeCredentialCreate
	lastConnector  fakeConnectorCreate
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

type fakeStatusUpdate struct {
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
	Type     string  `json:"type"`
	SystemID *string `json:"system_id"`
}

type fakeCustomerUpdate struct {
	BillingEnabled *bool `json:"billing_enabled"`
}

type fakeBillingUpdate struct {
	BillingMode       string `json:"billing_mode"`
	CreditLimitIsHard *bool  `json:"credit_limit_is_hard"`
}

type fakeBilling struct {
	enabled bool
	mode    string
	hard    *bool
}

type fakeBindPoolUpdate struct {
	BindPoolSize int `json:"bind_pool_size"`
}

type fakeAntispamRule struct {
	ID       string  `json:"id"`
	RuleType string  `json:"rule_type"`
	Scope    string  `json:"scope"`
	ScopeID  *string `json:"scope_id"`
	Status   string  `json:"status"`
}

type fakeExactRouteCreate struct {
	MSISDN     string `json:"msisdn"`
	TargetType string `json:"target_type"`
	TargetID   string `json:"target_id"`
	Source     string `json:"source"`
}

type fakeExactRouteImport struct {
	Source string                 `json:"source"`
	Rows   []fakeExactRouteCreate `json:"rows"`
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

// fakeBindPassword mints a secret the length a real bind password would be (8 chars — SMPP v3.4
// §4.1.1 bounds the field to 9 octets including the NUL terminator); a full UUID does not fit and a
// real SMPP peer would reject it as a malformed PDU before ever checking it.
func fakeBindPassword() string {
	return uuid.NewString()[:8]
}

func newFakeAdmin(t *testing.T) *fakeAdmin {
	t.Helper()
	f := &fakeAdmin{t: t, billing: map[string]fakeBilling{}}
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
	mux.HandleFunc("PATCH /v1/admin/smpp-accounts/{id}/credentials/{cid}", f.updateCredentialStatus)
	mux.HandleFunc("PATCH /v1/admin/customers/{id}", f.updateCustomer)
	mux.HandleFunc("PATCH /v1/admin/customers/{id}/billing", f.updateBilling)
	mux.HandleFunc("PATCH /v1/admin/connectors/{id}/bind-pool", f.setBindPool)
	mux.HandleFunc("GET /v1/admin/antispam-rules", f.listAntispamRules)
	mux.HandleFunc("POST /v1/admin/exact-routes/import", f.importExactRoutes)
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
	body, ok := decodeFakeBody[fakeStatusUpdate](w, r)
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
	f.lastAccount = body
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
		out = append(out, fakeCredentialResp{ID: c.id, Type: c.typ, SystemID: c.systemID, Status: c.status})
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
	f.lastCredential = body
	systemID := ""
	if body.SystemID != nil {
		systemID = *body.SystemID
	}
	if (body.Type == "smpp_bind") != (systemID != "") {
		http.Error(w, `{"code":"validation_error","message":"system_id requis pour smpp_bind seulement"}`, http.StatusUnprocessableEntity)
		return
	}
	for _, existing := range a.credentials {
		if existing.typ == body.Type {
			// credentials_one_per_type_uq: a revoked row keeps its slot (internal/storage/postgres/credentials.go).
			http.Error(w, `{"code":"conflict","message":"type déjà présent sur le compte"}`, http.StatusConflict)
			return
		}
	}
	c := &fakeCredentialRow{id: uuid.NewString(), typ: body.Type, systemID: systemID, status: "active", secret: fakeBindPassword()}
	a.credentials = append(a.credentials, c)
	f.writes++
	writeFakeJSON(w, http.StatusCreated, fakeCredentialResp{
		ID: c.id, Type: c.typ, SystemID: c.systemID, Status: c.status, Secret: c.secret,
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
		c.secret = fakeBindPassword()
		c.rotations++
		f.writes++
		writeFakeJSON(w, http.StatusOK, fakeCredentialResp{
			ID: c.id, Type: c.typ, SystemID: c.systemID, Status: c.status, Secret: c.secret,
		})
		return
	}
	http.NotFound(w, r)
}

func (f *fakeAdmin) updateCredentialStatus(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeStatusUpdate](w, r)
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
	for _, c := range a.credentials {
		if c.id == r.PathValue("cid") {
			c.status = body.Status
			f.writes++
			writeFakeJSON(w, http.StatusOK, fakeCredentialResp{ID: c.id, Type: c.typ, SystemID: c.systemID, Status: c.status})
			return
		}
	}
	http.NotFound(w, r)
}

func (f *fakeAdmin) updateCustomer(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeCustomerUpdate](w, r)
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
	if body.BillingEnabled != nil {
		b := f.billing[c.id]
		b.enabled = *body.BillingEnabled
		f.billing[c.id] = b
	}
	f.writes++
	writeFakeJSON(w, http.StatusOK, fakeCustomerResp{ID: c.id, Name: c.name})
}

func (f *fakeAdmin) updateBilling(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeBillingUpdate](w, r)
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
	b := f.billing[c.id]
	b.mode, b.hard = body.BillingMode, body.CreditLimitIsHard
	f.billing[c.id] = b
	f.writes++
	writeFakeJSON(w, http.StatusOK, map[string]any{"customer_id": c.id})
}

func (f *fakeAdmin) setBindPool(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeBindPoolUpdate](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.connectors {
		if f.connectors[i].id == r.PathValue("id") {
			f.connectors[i].bindPoolSize = body.BindPoolSize
			f.writes++
			writeFakeJSON(w, http.StatusOK, fakeConnectorResp{ID: f.connectors[i].id, Name: f.connectors[i].name})
			return
		}
	}
	http.NotFound(w, r)
}

func (f *fakeAdmin) listAntispamRules(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	writeFakeJSON(w, http.StatusOK, append([]fakeAntispamRule{}, f.antispamRules...))
}

func (f *fakeAdmin) importExactRoutes(w http.ResponseWriter, r *http.Request) {
	body, ok := decodeFakeBody[fakeExactRouteImport](w, r)
	if !ok {
		return
	}
	f.mu.Lock()
	f.exactRoutes = append(f.exactRoutes, body.Rows...)
	f.writes++
	f.mu.Unlock()
	writeFakeJSON(w, http.StatusAccepted, map[string]any{"job_id": uuid.NewString(), "status": "queued", "created_at": "2026-09-28T00:00:00Z"})
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
	f.lastConnector = body
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

func (f *fakeAdmin) credentialSecret(accountName, systemID string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.accounts {
		if a.name != accountName {
			continue
		}
		for _, c := range a.credentials {
			if c.systemID == systemID {
				return c.secret
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

func (f *fakeAdmin) customerID(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.customers {
		if c.name == name {
			return c.id
		}
	}
	return ""
}

func (f *fakeAdmin) bindPoolSize(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.connectors {
		if c.name == name {
			return c.bindPoolSize
		}
	}
	return 0
}

func (f *fakeAdmin) revokeAPIKey(accountName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.accounts {
		for _, c := range a.credentials {
			if a.name == accountName && c.typ == "api_key" {
				c.status = "revoked"
			}
		}
	}
}

func (f *fakeAdmin) activeAPIKeys(accountName string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, a := range f.accounts {
		if a.name != accountName {
			continue
		}
		for _, c := range a.credentials {
			if c.typ == "api_key" && c.status == "active" {
				n++
			}
		}
	}
	return n
}
