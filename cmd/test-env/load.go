package main

import (
	"context"
	"fmt"
	"net/http"
)

// k6Destinations mirrors msisdn() in test/load/k6/messages.js: +225070000 followed by four digits.
const k6Destinations = 10000

const loadAccountName = "load"

type loadSpec struct {
	Customers    int
	BindPoolSize int
	PortedShare  float64
}

type antispamRule struct {
	ID       string  `json:"id"`
	RuleType string  `json:"rule_type"`
	Scope    string  `json:"scope"`
	ScopeID  *string `json:"scope_id"`
	Status   string  `json:"status"`
}

type exactRouteCreate struct {
	MSISDN     string `json:"msisdn"`
	TargetType string `json:"target_type"`
	TargetID   string `json:"target_id"`
	Source     string `json:"source"`
}

// seedLoad prepares the load campaign's tenants and returns one fresh API key per customer: one tenant
// would measure one mt.inbound partition and one balance row (tasks-done/step-280.md).
func seedLoad(ctx context.Context, a *admin, c connectorSpec, spec loadSpec) ([]string, error) {
	connectorID, err := seed(ctx, a, c)
	if err != nil {
		return nil, err
	}
	if err := a.do(ctx, http.MethodPatch, "/connectors/"+connectorID+"/bind-pool", map[string]int{"bind_pool_size": spec.BindPoolSize}, nil); err != nil {
		return nil, fmt.Errorf("bind_pool_size : %w", err)
	}
	// Opt-in (§6.13): without it the first peer reset parks the connector until a manual rebind.
	if err := a.do(ctx, http.MethodPatch, "/connectors/"+connectorID+"/reconnect-policy", map[string]bool{"auto_reconnect_enabled": true}, nil); err != nil {
		return nil, fmt.Errorf("auto-reconnexion : %w", err)
	}
	if ported := int(spec.PortedShare * k6Destinations); ported > 0 {
		rows := make([]exactRouteCreate, ported)
		for n := range rows {
			rows[n] = exactRouteCreate{MSISDN: fmt.Sprintf("+225070000%04d", n), TargetType: "connector", TargetID: connectorID, Source: "mnp_import"}
		}
		body := map[string]any{"source": "mnp_import", "rows": rows}
		if err := a.do(ctx, http.MethodPost, "/exact-routes/import", body, nil); err != nil {
			return nil, fmt.Errorf("import des routes exactes : %w", err)
		}
	}

	keys := make([]string, spec.Customers)
	for i := range keys {
		name := fmt.Sprintf("load-%02d", i)
		if keys[i], err = seedLoadCustomer(ctx, a, name); err != nil {
			return nil, fmt.Errorf("client %q : %w", name, err)
		}
	}
	return keys, nil
}

func seedLoadCustomer(ctx context.Context, a *admin, name string) (string, error) {
	customerID, err := findOrCreateCustomer(ctx, a, name)
	if err != nil {
		return "", err
	}
	if err := ensureActiveSenderID(ctx, a, customerID, senderAddr); err != nil {
		return "", fmt.Errorf("sender ID %q : %w", senderAddr, err)
	}
	accountID, err := findOrCreateAccount(ctx, a, customerID, loadAccountName)
	if err != nil {
		return "", fmt.Errorf("compte %q : %w", loadAccountName, err)
	}
	if err := refuseRunFlaggingRules(ctx, a, customerID, accountID); err != nil {
		return "", err
	}
	// Postpaid before enabling: in the other order, a failed second PATCH leaves a strict prepaid
	// customer that blocks every message. credit_limit is left alone — a PATCH null does not clear it.
	postpaidSoft := map[string]any{"billing_mode": "postpaid", "credit_limit_is_hard": false}
	if err := a.do(ctx, http.MethodPatch, "/customers/"+customerID+"/billing", postpaidSoft, nil); err != nil {
		return "", fmt.Errorf("mode postpayé : %w", err)
	}
	if err := a.do(ctx, http.MethodPatch, "/customers/"+customerID, map[string]any{"billing_enabled": true}, nil); err != nil {
		return "", fmt.Errorf("activation de la facturation : %w", err)
	}
	return freshAPIKey(ctx, a, accountID)
}

// refuseRunFlaggingRules stops the campaign before it starts: k6 repeats one body over 10 000
// destinations, so a duplicate or velocity rule would flag the run silently after its first round.
func refuseRunFlaggingRules(ctx context.Context, a *admin, customerID, accountID string) error {
	var rules []antispamRule
	if err := a.do(ctx, http.MethodGet, "/antispam-rules", nil, &rules); err != nil {
		return fmt.Errorf("règles anti-spam : %w", err)
	}
	for _, r := range rules {
		if r.Status != "active" || (r.RuleType != "duplicate" && r.RuleType != "velocity") {
			continue
		}
		covers := r.Scope == "global" ||
			(r.ScopeID != nil && ((r.Scope == "customer" && *r.ScopeID == customerID) || (r.Scope == "smpp_account" && *r.ScopeID == accountID)))
		if covers {
			return fmt.Errorf("la règle anti-spam %s (%s, %s) couvre le compte de charge : la désactiver avant la campagne", r.ID, r.RuleType, r.Scope)
		}
	}
	return nil
}

// freshAPIKey rotates the account's api_key: a revoked row keeps its (account, type) slot, so creating
// a second one conflicts.
func freshAPIKey(ctx context.Context, a *admin, accountID string) (string, error) {
	var creds []credential
	if err := a.do(ctx, http.MethodGet, "/smpp-accounts/"+accountID+"/credentials", nil, &creds); err != nil {
		return "", err
	}
	var minted struct {
		Secret string `json:"secret"`
	}
	for _, c := range creds {
		if c.Type != "api_key" {
			continue
		}
		// Rotate before reactivating: the other order would briefly revive the old secret.
		path := "/smpp-accounts/" + accountID + "/credentials/" + c.ID
		if err := a.do(ctx, http.MethodPost, path+"/rotate", nil, &minted); err != nil {
			return "", fmt.Errorf("rotation de la clé %s : %w", c.ID, err)
		}
		if c.Status != "active" {
			if err := a.do(ctx, http.MethodPatch, path, map[string]string{"status": "active"}, nil); err != nil {
				return "", fmt.Errorf("réactivation de la clé %s : %w", c.ID, err)
			}
		}
		return minted.Secret, nil
	}
	if err := a.do(ctx, http.MethodPost, "/smpp-accounts/"+accountID+"/credentials", map[string]string{"type": "api_key"}, &minted); err != nil {
		return "", fmt.Errorf("création de la clé : %w", err)
	}
	return minted.Secret, nil
}
