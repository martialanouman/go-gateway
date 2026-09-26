package adminapi_test

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/connector/status"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// fakeCustomerStore is an in-memory CustomerStore for handler unit tests. It is a hand-written fake
// (guide-codage-go §6), not a mock framework: the handlers drive real branches — including a store
// returning ErrConflict — in milliseconds, without Docker.
type fakeCustomerStore struct {
	mu          sync.Mutex
	byID        map[uuid.UUID]cp.Customer
	order       []uuid.UUID
	createErr   error // when set, Create returns it (to drive 409/422 paths)
	setGroupErr error // when set, SetGroup returns it (the FK rejection behind set-customer-group's 422)
}

func newFakeCustomerStore() *fakeCustomerStore {
	return &fakeCustomerStore{byID: map[uuid.UUID]cp.Customer{}}
}

func (s *fakeCustomerStore) Create(_ context.Context, in cp.NewCustomer) (cp.Customer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.Customer{}, s.createErr
	}
	c := cp.Customer{
		ID:             uuid.New(),
		Name:           in.Name,
		Status:         cp.CustomerActive,
		GroupID:        in.GroupID,
		RatePlanID:     in.RatePlanID,
		BillingEnabled: in.BillingEnabled,
		BillingMode:    in.BillingMode,
		BalanceScope:   cp.BalanceScopeCustomer,
		ContentStorage: cp.ContentInherit,
	}
	if in.BalanceScope != nil {
		c.BalanceScope = *in.BalanceScope
	}
	if in.ContentStorage != nil {
		c.ContentStorage = *in.ContentStorage
	}
	s.byID[c.ID] = c
	s.order = append(s.order, c.ID)
	return c, nil
}

func (s *fakeCustomerStore) Get(_ context.Context, id uuid.UUID) (cp.Customer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[id]
	if !ok {
		return cp.Customer{}, errs.ErrNotFound
	}
	return c, nil
}

func (s *fakeCustomerStore) List(_ context.Context, f cp.CustomerFilter) (cp.Page[cp.Customer], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []cp.Customer
	skipping := f.After != uuid.Nil
	for _, id := range s.order {
		if skipping {
			skipping = id != f.After
			continue
		}
		c := s.byID[id]
		if f.Status != nil && c.Status != *f.Status {
			continue
		}
		// The group filter is modelled here because list-group-customers resolves membership through
		// it: a double that ignored it would return every customer and let a handler that forgot the
		// filter pass.
		if f.GroupID != nil && (c.GroupID == nil || *c.GroupID != *f.GroupID) {
			continue
		}
		items = append(items, c)
	}
	// The repository fetches Limit+1 rows: without a limit it returns one customer and no next page.
	limit := max(f.Limit, 1)
	if len(items) > limit && f.Limit > 0 {
		return cp.Page[cp.Customer]{Items: items[:limit], NextCursor: cp.EncodeCursor(items[limit-1].ID), HasMore: true}, nil
	}
	if len(items) > limit {
		return cp.Page[cp.Customer]{Items: items[:limit]}, nil
	}
	return cp.Page[cp.Customer]{Items: items}, nil
}

func (s *fakeCustomerStore) Update(_ context.Context, id uuid.UUID, p cp.CustomerPatch) (cp.Customer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[id]
	if !ok {
		return cp.Customer{}, errs.ErrNotFound
	}
	if p.Name != nil {
		c.Name = *p.Name
	}
	if p.Status != nil {
		c.Status = *p.Status
	}
	if p.ContentStorage != nil {
		c.ContentStorage = *p.ContentStorage
	}
	if p.ContentRetentionDays != nil {
		c.ContentRetentionDays = p.ContentRetentionDays
	}
	s.byID[id] = c
	return c, nil
}

func (s *fakeCustomerStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.byID, id)
	s.order = slices.DeleteFunc(s.order, func(o uuid.UUID) bool { return o == id })
	return nil
}

func (s *fakeCustomerStore) Suspend(_ context.Context, id uuid.UUID) (cp.Customer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[id]
	if !ok {
		return cp.Customer{}, errs.ErrNotFound
	}
	c.Status = cp.CustomerSuspended
	s.byID[id] = c
	return c, nil
}

// SetGroup mirrors the repository: a nil groupID CLEARS the membership rather than leaving it
// alone, and an unknown group is the FK rejection the caller injects through setGroupErr.
func (s *fakeCustomerStore) SetGroup(_ context.Context, id uuid.UUID, groupID *uuid.UUID) (cp.Customer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setGroupErr != nil {
		return cp.Customer{}, s.setGroupErr
	}
	c, ok := s.byID[id]
	if !ok {
		return cp.Customer{}, errs.ErrNotFound
	}
	c.GroupID = groupID
	s.byID[id] = c
	return c, nil
}

// fakeCustomerGroupStore is an in-memory CustomerGroupStore for handler unit tests.
type fakeCustomerGroupStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]cp.CustomerGroup
	order     []uuid.UUID
	createErr error // when set, Create returns it (to drive the 409 path)
}

func newFakeCustomerGroupStore() *fakeCustomerGroupStore {
	return &fakeCustomerGroupStore{byID: map[uuid.UUID]cp.CustomerGroup{}}
}

// seed inserts a group directly, for the tests that need one to already exist.
func (s *fakeCustomerGroupStore) seed(g cp.CustomerGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[g.ID] = g
	s.order = append(s.order, g.ID)
}

func (s *fakeCustomerGroupStore) Create(_ context.Context, in cp.NewCustomerGroup) (cp.CustomerGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.CustomerGroup{}, s.createErr
	}
	g := cp.CustomerGroup{
		ID:          uuid.New(),
		Name:        in.Name,
		Description: in.Description,
		Status:      cp.CustomerGroupActive,
	}
	s.byID[g.ID] = g
	s.order = append(s.order, g.ID)
	return g, nil
}

func (s *fakeCustomerGroupStore) Get(_ context.Context, id uuid.UUID) (cp.CustomerGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.byID[id]
	if !ok {
		return cp.CustomerGroup{}, errs.ErrNotFound
	}
	return g, nil
}

func (s *fakeCustomerGroupStore) List(_ context.Context, f cp.CustomerGroupFilter) ([]cp.CustomerGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.CustomerGroup, 0, len(s.order))
	for _, id := range s.order {
		g := s.byID[id]
		if f.Status != nil && g.Status != *f.Status {
			continue
		}
		out = append(out, g)
	}
	return out, nil
}

func (s *fakeCustomerGroupStore) Update(_ context.Context, id uuid.UUID, p cp.CustomerGroupPatch) (cp.CustomerGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.byID[id]
	if !ok {
		return cp.CustomerGroup{}, errs.ErrNotFound
	}
	if p.Name != nil {
		g.Name = *p.Name
	}
	if p.Description != nil {
		g.Description = p.Description
	}
	if p.Status != nil {
		g.Status = *p.Status
	}
	s.byID[id] = g
	return g, nil
}

func (s *fakeCustomerGroupStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.byID, id)
	s.order = slices.DeleteFunc(s.order, func(o uuid.UUID) bool { return o == id })
	return nil
}

// fakeAccountStore is an in-memory AccountStore for handler unit tests.
type fakeAccountStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]cp.Account
	createErr error
	// customers resolves the group filter, which is a property of the OWNING customer: the real
	// query filters on customer_id IN (SELECT id FROM customers WHERE group_id = ...). Left nil when
	// a test does not exercise ?groupId=.
	customers *fakeCustomerStore
}

func newFakeAccountStore() *fakeAccountStore {
	return &fakeAccountStore{byID: map[uuid.UUID]cp.Account{}}
}

func (s *fakeAccountStore) Create(_ context.Context, in cp.NewAccount) (cp.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.Account{}, s.createErr
	}
	a := cp.Account{
		ID:               uuid.New(),
		CustomerID:       in.CustomerID,
		Name:             in.Name,
		Status:           cp.AccountActive,
		SMPPEnabled:      boolOr(in.SMPPEnabled, true),
		RESTEnabled:      boolOr(in.RESTEnabled, true),
		SenderIDPolicy:   cp.SenderIDStrict,
		QuerySMEnabled:   boolOr(in.QuerySMEnabled, true),
		CancelSMEnabled:  boolOr(in.CancelSMEnabled, true),
		AllowedBindTypes: cp.BindTRX,
		MaxSessions:      1,
	}
	if in.MaxSessions != nil {
		a.MaxSessions = *in.MaxSessions
	}
	s.byID[a.ID] = a
	return a, nil
}

func (s *fakeAccountStore) Get(_ context.Context, id uuid.UUID) (cp.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.byID[id]
	if !ok {
		return cp.Account{}, errs.ErrNotFound
	}
	return a, nil
}

func (s *fakeAccountStore) List(ctx context.Context, f cp.AccountFilter) (cp.Page[cp.Account], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]cp.Account, 0, len(s.byID))
	for _, a := range s.byID {
		if f.GroupID != nil && !s.customerIsInGroup(ctx, a.CustomerID, *f.GroupID) {
			continue
		}
		items = append(items, a)
	}
	return cp.Page[cp.Account]{Items: items}, nil
}

// customerIsInGroup mirrors the sub-select the real query runs. Without a customer store there is
// no membership to resolve, so nothing matches — a silent "everything matches" would make a handler
// that dropped the filter look correct.
func (s *fakeAccountStore) customerIsInGroup(ctx context.Context, customerID, groupID uuid.UUID) bool {
	if s.customers == nil {
		// Returning false would answer an empty page, making any "no member" assertion pass without
		// the filter ever being exercised.
		panic("fakeAccountStore: ?groupId= needs .customers wired to resolve membership")
	}
	c, err := s.customers.Get(ctx, customerID)
	return err == nil && c.GroupID != nil && *c.GroupID == groupID
}

func (s *fakeAccountStore) Update(_ context.Context, id uuid.UUID, p cp.AccountPatch) (cp.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.byID[id]
	if !ok {
		return cp.Account{}, errs.ErrNotFound
	}
	if p.Name != nil {
		a.Name = *p.Name
	}
	if p.Status != nil {
		a.Status = *p.Status
	}
	s.byID[id] = a
	return a, nil
}

func (s *fakeAccountStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.byID, id)
	return nil
}

func (s *fakeAccountStore) SetChannels(_ context.Context, id uuid.UUID, smpp, rest bool) (cp.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.byID[id]
	if !ok {
		return cp.Account{}, errs.ErrNotFound
	}
	a.SMPPEnabled, a.RESTEnabled = smpp, rest
	s.byID[id] = a
	return a, nil
}

func (s *fakeAccountStore) SetSessionLimits(_ context.Context, id uuid.UUID, maxSessions int, bind cp.BindType) (cp.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.byID[id]
	if !ok {
		return cp.Account{}, errs.ErrNotFound
	}
	a.MaxSessions, a.AllowedBindTypes = maxSessions, bind
	s.byID[id] = a
	return a, nil
}

func (s *fakeAccountStore) Suspend(_ context.Context, id uuid.UUID) (cp.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.byID[id]
	if !ok {
		return cp.Account{}, errs.ErrNotFound
	}
	a.Status = cp.AccountSuspended
	s.byID[id] = a
	return a, nil
}

func boolOr(p *bool, fallback bool) bool {
	if p == nil {
		return fallback
	}
	return *p
}

// fakeCredentialStore is an in-memory CredentialStore for handler unit tests. It enforces the
// one-per-type cardinality so the 409 path can be exercised without Docker.
type fakeCredentialStore struct {
	mu    sync.Mutex
	byID  map[uuid.UUID]cp.Credential
	types map[string]bool // "<accountID>:<type>" occupancy, revoked slots stay occupied
	// lastRotation is what the handler passed to the most recent Rotate, so a test can assert the
	// grace_period_sec -> time.Duration conversion that is otherwise invisible.
	lastRotation *cp.CredentialRotation
}

func newFakeCredentialStore() *fakeCredentialStore {
	return &fakeCredentialStore{byID: map[uuid.UUID]cp.Credential{}, types: map[string]bool{}}
}

func (s *fakeCredentialStore) typeKey(account uuid.UUID, t cp.CredentialType) string {
	return account.String() + ":" + string(t)
}

func (s *fakeCredentialStore) Create(_ context.Context, in cp.NewCredential) (cp.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.types[s.typeKey(in.AccountID, in.Type)] {
		return cp.Credential{}, errs.ErrConflict
	}
	c := cp.Credential{
		ID:        uuid.New(),
		AccountID: in.AccountID,
		Type:      in.Type,
		SystemID:  in.SystemID,
		Status:    cp.CredentialActive,
	}
	s.byID[c.ID] = c
	s.types[s.typeKey(in.AccountID, in.Type)] = true
	return c, nil
}

func (s *fakeCredentialStore) ListByAccount(_ context.Context, accountID uuid.UUID) ([]cp.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.Credential, 0)
	for _, c := range s.byID {
		if c.AccountID == accountID {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s *fakeCredentialStore) Get(_ context.Context, accountID, credID uuid.UUID) (cp.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[credID]
	if !ok || c.AccountID != accountID {
		return cp.Credential{}, errs.ErrNotFound
	}
	return c, nil
}

func (s *fakeCredentialStore) SetStatus(_ context.Context, accountID, credID uuid.UUID, st cp.CredentialStatus) (cp.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[credID]
	if !ok || c.AccountID != accountID {
		return cp.Credential{}, errs.ErrNotFound
	}
	c.Status = st
	s.byID[credID] = c
	// The slot stays occupied even when revoked (decision 2).
	return c, nil
}

func (s *fakeCredentialStore) Rotate(_ context.Context, accountID, credID uuid.UUID, rot cp.CredentialRotation) (cp.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[credID]
	if !ok || c.AccountID != accountID {
		return cp.Credential{}, errs.ErrNotFound
	}
	// The rotation is recorded, not discarded: the handler's seconds -> Duration conversion is only
	// observable here, and getting it wrong turns every grace window into an immediate cutover.
	s.lastRotation = &rot
	now := time.Now()
	c.RotatedAt = &now
	if rot.Grace != nil {
		expiry := now.Add(*rot.Grace)
		c.GraceExpiresAt = &expiry
	}
	s.byID[credID] = c
	return c, nil
}

// rotation returns the CredentialRotation the handler last built, or fails the test if none was.
func (s *fakeCredentialStore) rotation(t *testing.T) cp.CredentialRotation {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastRotation == nil {
		t.Fatal("Rotate was never called")
	}
	return *s.lastRotation
}

// fakeConnectorStore is an in-memory ConnectorStore for handler unit tests.
type fakeConnectorStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]cp.Connector
	rateLimit map[uuid.UUID]cp.RateLimit // a connector's operational limit, for the throughput validation
	createErr error

	// created/createCount record what Create was ACTUALLY handed, so a test can assert on the value that
	// would reach the column rather than on the handler having been called.
	created     cp.NewConnector
	createCount int
	patched     cp.ConnectorPatch
}

func newFakeConnectorStore() *fakeConnectorStore {
	return &fakeConnectorStore{byID: map[uuid.UUID]cp.Connector{}, rateLimit: map[uuid.UUID]cp.RateLimit{}}
}

func (s *fakeConnectorStore) RateLimit(_ context.Context, connectorID uuid.UUID) (cp.RateLimit, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.rateLimit[connectorID]
	return l, ok, nil
}

func (s *fakeConnectorStore) Create(_ context.Context, in cp.NewConnector) (cp.Connector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.Connector{}, s.createErr
	}
	s.created = in
	s.createCount++
	c := cp.Connector{
		ID: uuid.New(), Name: in.Name, Host: in.Host, Port: in.Port,
		BindType: in.BindType, SystemID: in.SystemID, Status: cp.ConnectorActive,
		BindPoolSize: 1, WindowSize: 10, ReconnectMultiplier: 2.0,
	}
	s.byID[c.ID] = c
	return c, nil
}

func (s *fakeConnectorStore) Get(_ context.Context, id uuid.UUID) (cp.Connector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[id]
	if !ok {
		return cp.Connector{}, errs.ErrNotFound
	}
	return c, nil
}

func (s *fakeConnectorStore) List(_ context.Context) ([]cp.Connector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.Connector, 0, len(s.byID))
	for _, c := range s.byID {
		out = append(out, c)
	}
	return out, nil
}

func (s *fakeConnectorStore) Update(_ context.Context, id uuid.UUID, p cp.ConnectorPatch) (cp.Connector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[id]
	if !ok {
		return cp.Connector{}, errs.ErrNotFound
	}
	if p.Name != nil {
		c.Name = *p.Name
	}
	if p.Status != nil {
		c.Status = *p.Status
	}
	// The sealed password moves the way the real repository moves it, and the whole patch is kept: a test
	// asserting on a rotation would otherwise be asserting on this double's omission.
	if p.Password != nil {
		c.Password = *p.Password
	}
	s.patched = p
	s.byID[id] = c
	return c, nil
}

func (s *fakeConnectorStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.byID, id)
	return nil
}

func (s *fakeConnectorStore) UpdateReconnectPolicy(_ context.Context, id uuid.UUID, p cp.ReconnectPolicy) (cp.Connector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[id]
	if !ok {
		return cp.Connector{}, errs.ErrNotFound
	}
	c.AutoReconnectEnabled = p.AutoReconnectEnabled
	s.byID[id] = c
	return c, nil
}

func (s *fakeConnectorStore) UpdateBindPool(_ context.Context, id uuid.UUID, size int) (cp.Connector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.byID[id]
	if !ok {
		return cp.Connector{}, errs.ErrNotFound
	}
	c.BindPoolSize = size
	s.byID[id] = c
	return c, nil
}

// fakeConnectorControl is an in-memory ConnectorControl for handler unit tests.
type fakeConnectorControl struct {
	mu         sync.Mutex
	reconfigs  int
	statusByID map[uuid.UUID]status.Connector
}

func newFakeConnectorControl() *fakeConnectorControl {
	return &fakeConnectorControl{statusByID: map[uuid.UUID]status.Connector{}}
}

func (c *fakeConnectorControl) Read(_ context.Context, connectorID uuid.UUID) (status.Connector, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.statusByID[connectorID]
	if !ok {
		return status.Connector{ConnectorID: connectorID, BreakerState: "closed"}, nil
	}
	return st, nil
}

func (c *fakeConnectorControl) SignalReconfigure(_ context.Context, _ uuid.UUID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reconfigs++
	return nil
}

// fakeRouteStore is an in-memory RouteStore for handler unit tests.
type fakeRouteStore struct {
	mu   sync.Mutex
	byID map[uuid.UUID]cp.Route
}

func newFakeRouteStore() *fakeRouteStore {
	return &fakeRouteStore{byID: map[uuid.UUID]cp.Route{}}
}

func (s *fakeRouteStore) Create(_ context.Context, in cp.NewRoute) (cp.Route, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := cp.Route{
		ID: uuid.New(), Name: in.Name, DistributionStrategy: in.DistributionStrategy,
		TargetConnectorID: in.TargetConnectorID, Status: cp.RouteActive, Targets: in.Targets,
	}
	if in.Priority != nil {
		r.Priority = *in.Priority
	} else {
		r.Priority = 100
	}
	s.byID[r.ID] = r
	return r, nil
}

func (s *fakeRouteStore) Get(_ context.Context, id uuid.UUID) (cp.Route, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.byID[id]
	if !ok {
		return cp.Route{}, errs.ErrNotFound
	}
	return r, nil
}

func (s *fakeRouteStore) List(_ context.Context) ([]cp.Route, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.Route, 0, len(s.byID))
	for _, r := range s.byID {
		out = append(out, r)
	}
	return out, nil
}

func (s *fakeRouteStore) Update(_ context.Context, id uuid.UUID, p cp.RoutePatch) (cp.Route, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.byID[id]
	if !ok {
		return cp.Route{}, errs.ErrNotFound
	}
	if p.Name != nil {
		r.Name = *p.Name
	}
	if p.Status != nil {
		r.Status = *p.Status
	}
	if p.Targets != nil {
		r.Targets = p.Targets
	}
	s.byID[id] = r
	return r, nil
}

func (s *fakeRouteStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.byID, id)
	return nil
}

// fakeSenderIDStore is an in-memory SenderIDStore for handler unit tests.
type fakeSenderIDStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]cp.SenderID
	createErr error
}

func newFakeSenderIDStore() *fakeSenderIDStore {
	return &fakeSenderIDStore{byID: map[uuid.UUID]cp.SenderID{}}
}

func (s *fakeSenderIDStore) Create(_ context.Context, in cp.NewSenderID) (cp.SenderID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.SenderID{}, s.createErr
	}
	sid := cp.SenderID{
		ID: uuid.New(), CustomerID: in.CustomerID, Address: in.Address,
		Status: cp.SenderIDPendingCarrierApproval,
	}
	s.byID[sid.ID] = sid
	return sid, nil
}

func (s *fakeSenderIDStore) ListByCustomer(_ context.Context, customerID uuid.UUID) ([]cp.SenderID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.SenderID, 0)
	for _, sid := range s.byID {
		if sid.CustomerID == customerID {
			out = append(out, sid)
		}
	}
	return out, nil
}

func (s *fakeSenderIDStore) Update(_ context.Context, customerID, senderID uuid.UUID, p cp.SenderIDPatch) (cp.SenderID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sid, ok := s.byID[senderID]
	if !ok || sid.CustomerID != customerID {
		return cp.SenderID{}, errs.ErrNotFound
	}
	if p.Status != nil {
		sid.Status = *p.Status
	}
	s.byID[senderID] = sid
	return sid, nil
}

func (s *fakeSenderIDStore) Delete(_ context.Context, customerID, senderID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sid, ok := s.byID[senderID]
	if !ok || sid.CustomerID != customerID {
		return errs.ErrNotFound
	}
	delete(s.byID, senderID)
	return nil
}

// fakeInboundNumberStore is an in-memory InboundNumberStore for handler unit tests. createErr drives
// the 409/422 paths; Assign stores the pointer as given so a nil (clear to shared) is observable.
type fakeInboundNumberStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]cp.InboundNumber
	createErr error
}

func newFakeInboundNumberStore() *fakeInboundNumberStore {
	return &fakeInboundNumberStore{byID: map[uuid.UUID]cp.InboundNumber{}}
}

func (s *fakeInboundNumberStore) Create(_ context.Context, in cp.NewInboundNumber) (cp.InboundNumber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.InboundNumber{}, s.createErr
	}
	n := cp.InboundNumber{
		ID:          uuid.New(),
		Address:     in.Address,
		NumberType:  in.NumberType,
		CountryCode: in.CountryCode,
		MCCMNC:      in.MCCMNC,
		ConnectorID: in.ConnectorID,
		AccountID:   in.AccountID,
		Status:      cp.InboundNumberActive,
	}
	s.byID[n.ID] = n
	return n, nil
}

func (s *fakeInboundNumberStore) List(_ context.Context) ([]cp.InboundNumber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.InboundNumber, 0, len(s.byID))
	for _, n := range s.byID {
		out = append(out, n)
	}
	return out, nil
}

func (s *fakeInboundNumberStore) Update(_ context.Context, id uuid.UUID, p cp.InboundNumberPatch) (cp.InboundNumber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.byID[id]
	if !ok {
		return cp.InboundNumber{}, errs.ErrNotFound
	}
	if p.NumberType != nil {
		n.NumberType = *p.NumberType
	}
	if p.MCCMNC != nil {
		n.MCCMNC = p.MCCMNC
	}
	if p.ConnectorID != nil {
		n.ConnectorID = p.ConnectorID
	}
	if p.Status != nil {
		n.Status = *p.Status
	}
	s.byID[id] = n
	return n, nil
}

func (s *fakeInboundNumberStore) Delete(_ context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byID[id]; !ok {
		return errs.ErrNotFound
	}
	delete(s.byID, id)
	return nil
}

func (s *fakeInboundNumberStore) Assign(_ context.Context, id uuid.UUID, accountID *uuid.UUID) (cp.InboundNumber, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.byID[id]
	if !ok {
		return cp.InboundNumber{}, errs.ErrNotFound
	}
	// Stored verbatim: a nil clears the dedication (shared), which the handler must pass through from
	// an explicit JSON null.
	n.AccountID = accountID
	s.byID[id] = n
	return n, nil
}

// fakeInboundKeywordStore is an in-memory InboundKeywordStore for handler unit tests. Every operation
// is scoped by inbound_number_id: a keyword whose number differs from the path id is invisible, so the
// scoping the real query enforces is exercised without Docker.
type fakeInboundKeywordStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]cp.InboundKeyword
	order     []uuid.UUID
	createErr error
}

func newFakeInboundKeywordStore() *fakeInboundKeywordStore {
	return &fakeInboundKeywordStore{byID: map[uuid.UUID]cp.InboundKeyword{}}
}

func (s *fakeInboundKeywordStore) Create(_ context.Context, in cp.NewInboundKeyword) (cp.InboundKeyword, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.InboundKeyword{}, s.createErr
	}
	kw := cp.InboundKeyword{
		ID:              uuid.New(),
		InboundNumberID: in.InboundNumberID,
		Keyword:         in.Keyword,
		MatchType:       in.MatchType,
		AccountID:       in.AccountID,
		Priority:        in.Priority,
		Status:          cp.InboundKeywordActive,
	}
	s.byID[kw.ID] = kw
	s.order = append(s.order, kw.ID)
	return kw, nil
}

func (s *fakeInboundKeywordStore) ListByInboundNumber(_ context.Context, inboundNumberID uuid.UUID) ([]cp.InboundKeyword, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.InboundKeyword, 0)
	for _, id := range s.order {
		if kw := s.byID[id]; kw.InboundNumberID == inboundNumberID {
			out = append(out, kw)
		}
	}
	return out, nil
}

func (s *fakeInboundKeywordStore) Update(_ context.Context, inboundNumberID, keywordID uuid.UUID, p cp.InboundKeywordPatch) (cp.InboundKeyword, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kw, ok := s.byID[keywordID]
	if !ok || kw.InboundNumberID != inboundNumberID {
		return cp.InboundKeyword{}, errs.ErrNotFound
	}
	if p.Keyword != nil {
		kw.Keyword = *p.Keyword
	}
	if p.MatchType != nil {
		kw.MatchType = *p.MatchType
	}
	if p.AccountID != nil {
		kw.AccountID = *p.AccountID
	}
	if p.Priority != nil {
		kw.Priority = *p.Priority
	}
	if p.Status != nil {
		kw.Status = *p.Status
	}
	s.byID[keywordID] = kw
	return kw, nil
}

func (s *fakeInboundKeywordStore) Delete(_ context.Context, inboundNumberID, keywordID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	kw, ok := s.byID[keywordID]
	if !ok || kw.InboundNumberID != inboundNumberID {
		return errs.ErrNotFound
	}
	delete(s.byID, keywordID)
	return nil
}

// fakeUnroutedMOStore is an in-memory UnroutedMOStore for handler unit tests. items are newest-first;
// List applies a simple id-positioned keyset so the pagination handler can be exercised without a DB.
type fakeUnroutedMOStore struct {
	items    []cp.UnroutedMO
	gotAfter *cp.UnroutedMOKey
}

func (s *fakeUnroutedMOStore) List(_ context.Context, limit int, after *cp.UnroutedMOKey) ([]cp.UnroutedMO, error) {
	s.gotAfter = after
	start := 0
	if after != nil {
		for i, u := range s.items {
			if u.ID == after.ID {
				start = i + 1
				break
			}
		}
	}
	if start > len(s.items) {
		start = len(s.items)
	}
	end := start + limit
	if end > len(s.items) {
		end = len(s.items)
	}
	return append([]cp.UnroutedMO(nil), s.items[start:end]...), nil
}

// DeleteByMSISDN is the RGPD erasure hook (step-166); the listing tests do not exercise it.
func (s *fakeUnroutedMOStore) DeleteByMSISDN(context.Context, string) (int, error) { return 0, nil }

// setPassword puts a sealed password on a stored connector, so a read-path test sees what Postgres would
// actually return. Without it the field is always its zero value and a leak assertion proves nothing.
func (s *fakeConnectorStore) setPassword(id uuid.UUID, secret cp.SealedSecret) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.byID[id]
	c.Password = secret
	s.byID[id] = c
}

// fakeWebhookStore is an in-memory WebhookStore for handler unit tests. It is keyed by id but every
// read and write also takes the account, because that is the store's real contract: the Admin path
// carries both, and an id alone must never reach another account's row. It models the repository's
// create defaults too — an active status, an empty policy object and set timestamps — so a handler
// test sees the shape a response really carries.
type fakeWebhookStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]cp.Webhook
	order     []uuid.UUID
	createErr error // when set, Create returns it (to drive the 409 path)
}

func newFakeWebhookStore() *fakeWebhookStore {
	return &fakeWebhookStore{byID: map[uuid.UUID]cp.Webhook{}}
}

// seed inserts a webhook directly, for the tests that need one to already exist.
func (s *fakeWebhookStore) seed(wh cp.Webhook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[wh.ID] = wh
	s.order = append(s.order, wh.ID)
}

// mustGet reads a row past the handlers, to assert what a request wrote.
func (s *fakeWebhookStore) mustGet(t *testing.T, id uuid.UUID) cp.Webhook {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	wh, ok := s.byID[id]
	if !ok {
		t.Fatalf("webhook %s is not in the store", id)
	}
	return wh
}

func (s *fakeWebhookStore) List(_ context.Context, accountID uuid.UUID) ([]cp.Webhook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]cp.Webhook, 0, len(s.order))
	for _, id := range s.order {
		if wh := s.byID[id]; wh.AccountID == accountID {
			out = append(out, wh)
		}
	}
	return out, nil
}

func (s *fakeWebhookStore) Create(_ context.Context, in cp.NewWebhook) (cp.Webhook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.createErr != nil {
		return cp.Webhook{}, s.createErr
	}
	policy := in.RetryPolicyJSON
	if policy == nil {
		policy = json.RawMessage("{}") // the CreateWebhook query's COALESCE default
	}
	now := time.Now()
	wh := cp.Webhook{
		ID:              uuid.New(),
		AccountID:       in.AccountID,
		EventType:       in.EventType,
		URL:             in.URL,
		Secret:          in.Secret,
		RetryPolicyJSON: policy,
		Status:          cp.WebhookActive,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	s.byID[wh.ID] = wh
	s.order = append(s.order, wh.ID)
	return wh, nil
}

func (s *fakeWebhookStore) Update(_ context.Context, accountID, id uuid.UUID, p cp.WebhookPatch) (cp.Webhook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wh, ok := s.byID[id]
	if !ok || wh.AccountID != accountID {
		return cp.Webhook{}, errs.ErrNotFound
	}
	if p.URL != nil {
		wh.URL = *p.URL
	}
	if p.Secret != nil {
		wh.Secret = *p.Secret
	}
	if p.RetryPolicyJSON != nil {
		wh.RetryPolicyJSON = p.RetryPolicyJSON
	}
	if p.Status != nil {
		wh.Status = *p.Status
	}
	wh.UpdatedAt = time.Now() // the webhooks_touch trigger
	s.byID[id] = wh
	return wh, nil
}

func (s *fakeWebhookStore) Delete(_ context.Context, accountID, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	wh, ok := s.byID[id]
	if !ok || wh.AccountID != accountID {
		return errs.ErrNotFound
	}
	delete(s.byID, id)
	s.order = slices.DeleteFunc(s.order, func(o uuid.UUID) bool { return o == id })
	return nil
}
