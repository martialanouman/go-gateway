package adminapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/auth"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

// subjectVerifier authenticates operatorToken as the given subject, the way OIDCVerifier hands over a
// token's sub.
type subjectVerifier struct{ subject string }

func (v subjectVerifier) Verify(_ context.Context, token string) (auth.Principal, error) {
	if token != operatorToken {
		return auth.Principal{}, errs.ErrUnauthenticated
	}
	return auth.Principal{Subject: v.subject, Scopes: []auth.Scope{auth.ScopeAdminRead, auth.ScopeAdminWrite}}, nil
}

// TestCreationsRecordTheOperatorAsCreatedBy: every creation whose resource carries created_by writes the
// principal's subject when it is an operator id (ADR-0019), and nothing for a static token's tok_… subject.
func TestCreationsRecordTheOperatorAsCreatedBy(t *testing.T) {
	customers := newFakeCustomerStore()
	owner, _ := customers.Create(t.Context(), newCustomerInput("Owner"))
	creations := []struct{ name, path, body string }{
		{"customer group", "/v1/admin/customer-groups", `{"name":"Carriers-%s"}`},
		{"routing script", "/v1/admin/routing-scripts",
			`{"scope":"platform","name":"night-%s","language":"js","source_code":"function resolveRoute(m){return null}"}`},
		{"sender rewrite rule", rewritePath, `{"scope":"platform","rewrite_type":"sanitize","reason":"%s"}`},
		{"sender id", "/v1/admin/customers/" + owner.ID.String() + "/sender-ids", `{"address":"A%.8s"}`},
	}
	operator := uuid.NewString()
	for _, tt := range []struct {
		name    string
		subject string
		want    any
	}{
		{"operator id", operator, operator},
		{"static token fingerprint", auth.Fingerprint(operatorToken), nil},
	} {
		deps := adminapi.Deps{
			Customers: customers, CustomerGroups: newFakeCustomerGroupStore(), RoutingScripts: newFakeRoutingScriptStore(),
			SenderRewriteRules: newFakeRewriteStore(), SenderIDs: newFakeSenderIDStore(),
			Verifier: subjectVerifier{tt.subject},
		}
		api, _ := adminapi.New(deps)
		for _, c := range creations {
			t.Run(tt.name+"/"+c.name, func(t *testing.T) {
				w := httptest.NewRecorder()
				api.ServeHTTP(w, authed(t, http.MethodPost, c.path, fmt.Sprintf(c.body, uuid.NewString())))
				if w.Code != http.StatusCreated {
					t.Fatalf("status = %d, want 201; body=%s", w.Code, w.Body)
				}
				var got map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got["created_by"] != tt.want {
					t.Errorf("created_by = %v, want %v", got["created_by"], tt.want)
				}
			})
		}
	}
}
