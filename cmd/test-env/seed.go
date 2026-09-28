package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

const (
	customerName  = "test"
	senderAddr    = "TEST"
	accountName   = "smoke"
	smokeSystemID = "smoke"
	connectorName = "smsc-simulator"
	routeName     = "test-catch-all"
)

type connectorSpec struct {
	Host     string
	Port     int
	SystemID string
	Password string
}

type customer struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type customerCreate struct {
	Name string `json:"name"`
}

type pageMeta struct {
	NextCursor *string `json:"next_cursor"`
	HasMore    bool    `json:"has_more"`
}

type customerPage struct {
	pageMeta
	Data []customer `json:"data"`
}

type senderID struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Status  string `json:"status"`
}

type senderIDCreate struct {
	Address string `json:"address"`
}

type senderIDUpdate struct {
	Status string `json:"status"`
}

type smppAccount struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type smppAccountCreate struct {
	CustomerID       string `json:"customer_id"`
	Name             string `json:"name"`
	AllowedBindTypes string `json:"allowed_bind_types"`
	MaxSessions      int    `json:"max_sessions"`
}

type credential struct {
	ID       string `json:"id"`
	SystemID string `json:"system_id"`
	Status   string `json:"status"`
}

type credentialCreate struct {
	Type     string `json:"type"`
	SystemID string `json:"system_id"`
}

type connector struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type connectorCreate struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	BindType string `json:"bind_type"`
	SystemID string `json:"system_id"`
	Password string `json:"password"`
}

type route struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	TargetConnectorID string `json:"target_connector_id"`
}

type routeCreate struct {
	Name                 string `json:"name"`
	Priority             int    `json:"priority"`
	DistributionStrategy string `json:"distribution_strategy"`
	TargetConnectorID    string `json:"target_connector_id"`
}

// seed provisions the control plane the smoke test needs — customer, sender ID, SMPP account,
// credential, connector, route — creating only what is missing, and returns the connector's id.
func seed(ctx context.Context, a *admin, c connectorSpec) (string, error) {
	customerID, err := findOrCreateCustomer(ctx, a, customerName)
	if err != nil {
		return "", fmt.Errorf("client %q : %w", customerName, err)
	}

	if err := ensureActiveSenderID(ctx, a, customerID, senderAddr); err != nil {
		return "", fmt.Errorf("sender ID %q : %w", senderAddr, err)
	}

	accountID, err := findOrCreateAccount(ctx, a, customerID, accountName)
	if err != nil {
		return "", fmt.Errorf("compte %q : %w", accountName, err)
	}

	if err := ensureCredential(ctx, a, accountID); err != nil {
		return "", fmt.Errorf("credential %q : %w", smokeSystemID, err)
	}

	connectorID, err := findOrCreateConnector(ctx, a, connectorName, c)
	if err != nil {
		return "", fmt.Errorf("connecteur %q : %w", connectorName, err)
	}

	if err := findOrCreateRoute(ctx, a, routeName, connectorID); err != nil {
		return "", fmt.Errorf("route %q : %w", routeName, err)
	}

	return connectorID, nil
}

func findSmokeAccount(ctx context.Context, a *admin) (string, error) {
	customerID, err := findCustomerID(ctx, a, customerName)
	if err != nil {
		return "", fmt.Errorf("client %q : %w", customerName, err)
	}
	if customerID == "" {
		return "", fmt.Errorf("client %q : introuvable", customerName)
	}
	accountID, err := findAccountID(ctx, a, customerID, accountName)
	if err != nil {
		return "", fmt.Errorf("compte %q : %w", accountName, err)
	}
	if accountID == "" {
		return "", fmt.Errorf("compte %q : introuvable", accountName)
	}
	return accountID, nil
}

func findCustomerID(ctx context.Context, a *admin, name string) (string, error) {
	cursor := ""
	for {
		q := url.Values{"limit": {"500"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var page customerPage
		if err := a.do(ctx, http.MethodGet, "/customers?"+q.Encode(), nil, &page); err != nil {
			return "", err
		}
		for _, cust := range page.Data {
			if cust.Name == name {
				return cust.ID, nil
			}
		}
		if !page.HasMore || page.NextCursor == nil {
			return "", nil
		}
		cursor = *page.NextCursor
	}
}

func findOrCreateCustomer(ctx context.Context, a *admin, name string) (string, error) {
	id, err := findCustomerID(ctx, a, name)
	if err != nil {
		return "", err
	}
	if id != "" {
		return id, nil
	}
	var created customer
	if err := a.do(ctx, http.MethodPost, "/customers", customerCreate{Name: name}, &created); err != nil {
		return "", err
	}
	return created.ID, nil
}

func ensureActiveSenderID(ctx context.Context, a *admin, customerID, address string) error {
	var senders []senderID
	if err := a.do(ctx, http.MethodGet, "/customers/"+customerID+"/sender-ids", nil, &senders); err != nil {
		return err
	}
	for _, s := range senders {
		if s.Address != address {
			continue
		}
		if s.Status == "active" {
			return nil
		}
		return a.do(ctx, http.MethodPatch, "/customers/"+customerID+"/sender-ids/"+s.ID, senderIDUpdate{Status: "active"}, nil)
	}
	var created senderID
	if err := a.do(ctx, http.MethodPost, "/customers/"+customerID+"/sender-ids", senderIDCreate{Address: address}, &created); err != nil {
		return err
	}
	return a.do(ctx, http.MethodPatch, "/customers/"+customerID+"/sender-ids/"+created.ID, senderIDUpdate{Status: "active"}, nil)
}

func findAccountID(ctx context.Context, a *admin, customerID, name string) (string, error) {
	var accounts []smppAccount
	if err := a.do(ctx, http.MethodGet, "/customers/"+customerID+"/smpp-accounts", nil, &accounts); err != nil {
		return "", err
	}
	for _, acc := range accounts {
		if acc.Name == name {
			return acc.ID, nil
		}
	}
	return "", nil
}

func findOrCreateAccount(ctx context.Context, a *admin, customerID, name string) (string, error) {
	id, err := findAccountID(ctx, a, customerID, name)
	if err != nil {
		return "", err
	}
	if id != "" {
		return id, nil
	}
	// max_sessions: 2, not 1 — the previous deployment's smoke run may still hold its session for the
	// unbind grace period, and a strict cap of 1 would refuse the new bind (invariant d).
	body := smppAccountCreate{CustomerID: customerID, Name: name, AllowedBindTypes: "trx", MaxSessions: 2}
	var created smppAccount
	if err := a.do(ctx, http.MethodPost, "/smpp-accounts", body, &created); err != nil {
		return "", err
	}
	return created.ID, nil
}

func activeCredentialID(ctx context.Context, a *admin, accountID string) (string, error) {
	var creds []credential
	if err := a.do(ctx, http.MethodGet, "/smpp-accounts/"+accountID+"/credentials", nil, &creds); err != nil {
		return "", err
	}
	for _, c := range creds {
		if c.SystemID == smokeSystemID && c.Status == "active" {
			return c.ID, nil
		}
	}
	return "", nil
}

func ensureCredential(ctx context.Context, a *admin, accountID string) error {
	id, err := activeCredentialID(ctx, a, accountID)
	if err != nil || id != "" {
		return err
	}
	body := credentialCreate{Type: "smpp_bind", SystemID: smokeSystemID}
	return a.do(ctx, http.MethodPost, "/smpp-accounts/"+accountID+"/credentials", body, nil)
}

func findOrCreateConnector(ctx context.Context, a *admin, name string, spec connectorSpec) (string, error) {
	var connectors []connector
	if err := a.do(ctx, http.MethodGet, "/connectors", nil, &connectors); err != nil {
		return "", err
	}
	for _, c := range connectors {
		if c.Name == name {
			return c.ID, nil
		}
	}
	body := connectorCreate{
		Name: name, Host: spec.Host, Port: spec.Port,
		BindType: "trx", SystemID: spec.SystemID, Password: spec.Password,
	}
	var created connector
	if err := a.do(ctx, http.MethodPost, "/connectors", body, &created); err != nil {
		return "", err
	}
	return created.ID, nil
}

func findOrCreateRoute(ctx context.Context, a *admin, name, connectorID string) error {
	var routes []route
	if err := a.do(ctx, http.MethodGet, "/routes", nil, &routes); err != nil {
		return err
	}
	for _, r := range routes {
		if r.Name == name {
			return nil
		}
	}
	body := routeCreate{Name: name, Priority: 100, DistributionStrategy: "static", TargetConnectorID: connectorID}
	return a.do(ctx, http.MethodPost, "/routes", body, nil)
}
