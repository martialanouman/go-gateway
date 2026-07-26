package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/antispam"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// antispamRuleDTO is the wire form of an AntispamRule (contract schema AntispamRule). config_json is a
// free-form, rule-type-specific object.
type antispamRuleDTO struct {
	ID         string         `json:"id" format:"uuid"`
	RuleType   string         `json:"rule_type" enum:"velocity,content_blacklist,duplicate,reputation"`
	Scope      string         `json:"scope" enum:"global,customer,smpp_account"`
	ScopeID    *string        `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	ConfigJSON map[string]any `json:"config_json,omitempty"`
	Action     string         `json:"action" enum:"block,flag,throttle"`
	Status     string         `json:"status" enum:"active,disabled"`
	CreatedAt  *time.Time     `json:"created_at,omitempty" format:"date-time"`
	UpdatedAt  *time.Time     `json:"updated_at,omitempty" format:"date-time"`
}

func toAntispamRuleDTO(r cp.AntispamRule) antispamRuleDTO {
	return antispamRuleDTO{
		ID:         idString(r.ID),
		RuleType:   string(r.RuleType),
		Scope:      string(r.Scope),
		ScopeID:    idPtr(r.ScopeID),
		ConfigJSON: rawToMap(r.ConfigJSON),
		Action:     string(r.Action),
		Status:     string(r.Status),
		CreatedAt:  ptr(r.CreatedAt),
		UpdatedAt:  ptr(r.UpdatedAt),
	}
}

// rawToMap decodes a stored config_json object for the response DTO. A malformed stored value (which
// the write path prevents) renders as an empty object rather than failing the read.
func rawToMap(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]any{}
	}
	return m
}

type antispamRuleCreateBody struct {
	RuleType   string         `json:"rule_type" enum:"velocity,content_blacklist,duplicate,reputation"`
	Scope      string         `json:"scope" enum:"global,customer,smpp_account"`
	ScopeID    *string        `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	ConfigJSON map[string]any `json:"config_json,omitempty"`
	Action     string         `json:"action" enum:"block,flag,throttle"`
}

type antispamRuleUpdateBody struct {
	ConfigJSON map[string]any `json:"config_json,omitempty"`
	Action     *string        `json:"action,omitempty" enum:"block,flag,throttle"`
	Status     *string        `json:"status,omitempty" enum:"active,disabled"`
}

// AntispamRuleStore is the persistence the anti-spam rule handlers need (declared consumer-side).
// *postgres.AntispamRuleRepo satisfies it.
type AntispamRuleStore interface {
	List(ctx context.Context) ([]cp.AntispamRule, error)
	Get(ctx context.Context, id uuid.UUID) (cp.AntispamRule, error)
	Create(ctx context.Context, in cp.NewAntispamRule) (cp.AntispamRule, error)
	Update(ctx context.Context, id uuid.UUID, p cp.AntispamRulePatch) (cp.AntispamRule, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

type antispamRuleHandlers struct {
	store AntispamRuleStore
}

func registerAntispamRules(api huma.API, store AntispamRuleStore) {
	h := &antispamRuleHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-antispam-rules", Method: http.MethodGet, Path: "/admin/antispam-rules",
		Summary: "List anti-spam rules", Tags: []string{"Anti-spam"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-antispam-rule", Method: http.MethodPost, Path: "/admin/antispam-rules",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create an anti-spam rule", Tags: []string{"Anti-spam"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "update-antispam-rule", Method: http.MethodPatch, Path: "/admin/antispam-rules/{id}",
		Summary: "Update an anti-spam rule", Tags: []string{"Anti-spam"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-antispam-rule", Method: http.MethodDelete, Path: "/admin/antispam-rules/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete an anti-spam rule", Tags: []string{"Anti-spam"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)
}

type listAntispamRulesOutput struct {
	Body []antispamRuleDTO
}

func (h *antispamRuleHandlers) list(ctx context.Context, _ *struct{}) (*listAntispamRulesOutput, error) {
	rules, err := h.store.List(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listAntispamRulesOutput{Body: make([]antispamRuleDTO, 0, len(rules))}
	for _, r := range rules {
		out.Body = append(out.Body, toAntispamRuleDTO(r))
	}
	return out, nil
}

type createAntispamRuleInput struct{ Body antispamRuleCreateBody }
type antispamRuleOutput struct{ Body antispamRuleDTO }

func (h *antispamRuleHandlers) create(ctx context.Context, in *createAntispamRuleInput) (*antispamRuleOutput, error) {
	scope := cp.AntispamScope(in.Body.Scope)
	scopeID, err := parseIDPtr("scope_id", in.Body.ScopeID)
	if err != nil {
		return nil, err
	}
	if err := validateScopePairing(scope, scopeID); err != nil {
		return nil, err
	}
	ruleType := cp.AntispamRuleType(in.Body.RuleType)
	config, err := validateAntispamConfig(ruleType, in.Body.ConfigJSON)
	if err != nil {
		return nil, err
	}
	r, err := h.store.Create(ctx, cp.NewAntispamRule{
		RuleType:   ruleType,
		Scope:      scope,
		ScopeID:    scopeID,
		ConfigJSON: config,
		Action:     cp.AntispamAction(in.Body.Action),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &antispamRuleOutput{Body: toAntispamRuleDTO(r)}, nil
}

type updateAntispamRuleInput struct {
	ID   string `path:"id" format:"uuid"`
	Body antispamRuleUpdateBody
}

func (h *antispamRuleHandlers) update(ctx context.Context, in *updateAntispamRuleInput) (*antispamRuleOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("anti-spam rule")
	}

	patch := cp.AntispamRulePatch{
		Action: enumPtr[cp.AntispamAction](in.Body.Action),
		Status: enumPtr[cp.AntispamRuleStatus](in.Body.Status),
	}
	// A new config_json is validated against the rule's immutable rule_type, so a bad regex/threshold
	// is rejected here rather than silently dropped by the engine at reload.
	if in.Body.ConfigJSON != nil {
		existing, gerr := h.store.Get(ctx, id)
		if gerr != nil {
			return nil, humaerr.FromError(gerr)
		}
		config, verr := validateAntispamConfig(existing.RuleType, in.Body.ConfigJSON)
		if verr != nil {
			return nil, verr
		}
		patch.ConfigJSON = config
	}

	r, err := h.store.Update(ctx, id, patch)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &antispamRuleOutput{Body: toAntispamRuleDTO(r)}, nil
}

type antispamRuleIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *antispamRuleHandlers) delete(ctx context.Context, in *antispamRuleIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("anti-spam rule")
	}
	if err := h.store.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

// validateScopePairing enforces antispam_scope_ck at the edge (a clear 422 instead of a DB CHECK
// violation): global carries no scope_id, every other scope requires one.
func validateScopePairing(scope cp.AntispamScope, scopeID *uuid.UUID) error {
	if scope == cp.AntispamScopeGlobal && scopeID != nil {
		return humaerr.FailValidation("invalid scope",
			humaerr.FieldError{Field: "scope_id", Message: "must be null for the global scope"})
	}
	if scope != cp.AntispamScopeGlobal && scopeID == nil {
		return humaerr.FailValidation("invalid scope",
			humaerr.FieldError{Field: "scope_id", Message: "is required for a customer or account scope"})
	}
	return nil
}

// validateAntispamConfig marshals the wire config to JSON and validates it against the rule type via
// the engine's own validator (the single source of truth), returning a 422 on a bad regex/threshold.
func validateAntispamConfig(ruleType cp.AntispamRuleType, config map[string]any) (json.RawMessage, error) {
	if config == nil {
		config = map[string]any{} // marshal to "{}" (the column default), never jsonb 'null'
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return nil, humaerr.Fail(errs.ErrValidation, "invalid config_json")
	}
	if err := antispam.ValidateRuleConfig(ruleType, raw); err != nil {
		return nil, humaerr.FailValidation("invalid config_json",
			humaerr.FieldError{Field: "config_json", Message: err.Error()})
	}
	return raw, nil
}
