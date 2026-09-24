package adminapi

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline/senderrewrite"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// senderRewriteRuleDTO is the wire form of a SenderRewriteRule (contract schema SenderRewriteRule).
type senderRewriteRuleDTO struct {
	ID                 string         `json:"id" format:"uuid"`
	Scope              string         `json:"scope" enum:"platform,customer,smpp_account,connector"`
	ScopeID            *string        `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	Direction          string         `json:"direction" enum:"mt,mo"`
	MatchSenderPattern *string        `json:"match_sender_pattern,omitempty" nullable:"true"`
	MatchDestPattern   *string        `json:"match_dest_pattern,omitempty" nullable:"true"`
	RewriteType        string         `json:"rewrite_type" enum:"static,fallback_pool,truncate,sanitize"`
	RewriteTo          *string        `json:"rewrite_to,omitempty" nullable:"true"`
	FallbackPool       []string       `json:"fallback_pool_json,omitempty" nullable:"true"`
	MaxLength          *int32         `json:"max_length,omitempty" minimum:"1" nullable:"true"`
	SanitizeCharset    map[string]any `json:"sanitize_charset_json,omitempty" nullable:"true"`
	Priority           int32          `json:"priority"`
	Reason             *string        `json:"reason,omitempty" nullable:"true"`
	Status             string         `json:"status" enum:"active,disabled"`
	CreatedBy          *string        `json:"created_by,omitempty" format:"uuid" nullable:"true"`
	CreatedAt          *time.Time     `json:"created_at,omitempty" format:"date-time"`
	UpdatedAt          *time.Time     `json:"updated_at,omitempty" format:"date-time"`
}

func toSenderRewriteRuleDTO(r cp.SenderRewriteRule) senderRewriteRuleDTO {
	dto := senderRewriteRuleDTO{
		ID:                 idString(r.ID),
		Scope:              string(r.Scope),
		ScopeID:            idPtr(r.ScopeID),
		Direction:          r.Direction,
		MatchSenderPattern: r.MatchSenderPattern,
		MatchDestPattern:   r.MatchDestPattern,
		RewriteType:        string(r.RewriteType),
		RewriteTo:          r.RewriteTo,
		FallbackPool:       r.FallbackPool,
		MaxLength:          r.MaxLength,
		Priority:           r.Priority,
		Reason:             r.Reason,
		Status:             r.Status,
		CreatedBy:          idPtr(r.CreatedBy),
		CreatedAt:          ptr(r.CreatedAt),
		UpdatedAt:          ptr(r.UpdatedAt),
	}
	if len(r.SanitizeCharset) > 0 {
		dto.SanitizeCharset = rawToMap(r.SanitizeCharset)
	}
	return dto
}

type senderRewriteRuleCreateBody struct {
	Scope              string         `json:"scope" enum:"platform,customer,smpp_account,connector"`
	ScopeID            *string        `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	Direction          string         `json:"direction,omitempty" enum:"mt,mo" doc:"Only mt is accepted: no engine evaluates mo rules yet."`
	MatchSenderPattern *string        `json:"match_sender_pattern,omitempty" nullable:"true" doc:"RE2 regex matched against the whole address, implicitly anchored; null matches any. Unlike a route match_dest_pattern, not a digit prefix."`
	MatchDestPattern   *string        `json:"match_dest_pattern,omitempty" nullable:"true" doc:"RE2 regex matched against the whole address, implicitly anchored; null matches any. Unlike a route match_dest_pattern, not a digit prefix."`
	RewriteType        string         `json:"rewrite_type" enum:"static,fallback_pool,truncate,sanitize"`
	RewriteTo          *string        `json:"rewrite_to,omitempty" nullable:"true" doc:"Required when rewrite_type = static."`
	FallbackPool       []string       `json:"fallback_pool_json,omitempty" nullable:"true" doc:"Required, non-empty, when rewrite_type = fallback_pool."`
	MaxLength          *int32         `json:"max_length,omitempty" minimum:"1" nullable:"true" doc:"Required when rewrite_type = truncate."`
	SanitizeCharset    map[string]any `json:"sanitize_charset_json,omitempty" nullable:"true" doc:"Read when rewrite_type is sanitize: an object whose single key allowed holds the characters kept, listed one by one (not ranges); every other character is removed. Null keeps ASCII letters and digits."`
	Priority           *int32         `json:"priority,omitempty" default:"100" doc:"Lower is evaluated first, within a scope."`
	Reason             *string        `json:"reason,omitempty" nullable:"true"`
}

type senderRewriteRuleUpdateBody struct {
	MatchSenderPattern *string        `json:"match_sender_pattern,omitempty" nullable:"true" doc:"RE2 regex matched against the whole address, implicitly anchored; null matches any. Unlike a route match_dest_pattern, not a digit prefix."`
	MatchDestPattern   *string        `json:"match_dest_pattern,omitempty" nullable:"true" doc:"RE2 regex matched against the whole address, implicitly anchored; null matches any. Unlike a route match_dest_pattern, not a digit prefix."`
	RewriteType        *string        `json:"rewrite_type,omitempty" enum:"static,fallback_pool,truncate,sanitize"`
	RewriteTo          *string        `json:"rewrite_to,omitempty" nullable:"true"`
	FallbackPool       []string       `json:"fallback_pool_json,omitempty" nullable:"true"`
	MaxLength          *int32         `json:"max_length,omitempty" minimum:"1" nullable:"true"`
	SanitizeCharset    map[string]any `json:"sanitize_charset_json,omitempty" nullable:"true" doc:"Read when rewrite_type is sanitize: an object whose single key allowed holds the characters kept, listed one by one (not ranges); every other character is removed. Null keeps ASCII letters and digits."`
	Priority           *int32         `json:"priority,omitempty"`
	Reason             *string        `json:"reason,omitempty" nullable:"true"`
	Status             *string        `json:"status,omitempty" enum:"active,disabled"`
}

// SenderRewriteRuleStore is the persistence the sender rewrite rule handlers need.
// *postgres.SenderRewriteRuleRepo satisfies it.
type SenderRewriteRuleStore interface {
	List(ctx context.Context, f cp.SenderRewriteFilter) ([]cp.SenderRewriteRule, error)
	Get(ctx context.Context, id uuid.UUID) (cp.SenderRewriteRule, error)
	Create(ctx context.Context, in cp.NewSenderRewriteRule) (cp.SenderRewriteRule, error)
	Update(ctx context.Context, id uuid.UUID, p cp.SenderRewriteRulePatch) (cp.SenderRewriteRule, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

type senderRewriteHandlers struct {
	store SenderRewriteRuleStore
}

func registerSenderRewriteRules(api huma.API, store SenderRewriteRuleStore) {
	h := &senderRewriteHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-sender-rewrite-rules", Method: http.MethodGet, Path: "/admin/sender-rewrite-rules",
		Summary: "List sender-ID rewrite rules", Tags: []string{"Sender Rewrite"},
		Description: "In evaluation order: scope (connector, smpp_account, customer, platform), then priority.",
		Security:    scopeSecurity(auth.ScopeAdminRead),
		Errors:      []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-sender-rewrite-rule", Method: http.MethodPost, Path: "/admin/sender-rewrite-rules",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create a rewrite rule", Tags: []string{"Sender Rewrite"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "update-sender-rewrite-rule", Method: http.MethodPatch, Path: "/admin/sender-rewrite-rules/{id}",
		Summary: "Update a rewrite rule", Tags: []string{"Sender Rewrite"},
		Description: "Validated on the rule the patch leaves behind, since rewrite_type is mutable.",
		Security:    scopeSecurity(auth.ScopeAdminWrite),
		Errors:      []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-sender-rewrite-rule", Method: http.MethodDelete, Path: "/admin/sender-rewrite-rules/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a rewrite rule", Tags: []string{"Sender Rewrite"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)

	register(api, huma.Operation{
		OperationID: "test-sender-rewrite-rule", Method: http.MethodPost, Path: "/admin/sender-rewrite-rules/{id}/test",
		Summary: "Test a rewrite rule against a sample", Tags: []string{"Sender Rewrite"},
		Description: "Runs this rule alone — whatever its status, without the precedence of the others — through the code connector-pool applies before the submit_sm. Writes nothing.",
		Security:    scopeSecurity(auth.ScopeAdminRead),
		Errors:      []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.test)
}

type listSenderRewriteRulesInput struct {
	Scope   string `query:"scope" enum:"platform,customer,smpp_account,connector"`
	ScopeID string `query:"scopeId" format:"uuid"`
}

type listSenderRewriteRulesOutput struct {
	Body []senderRewriteRuleDTO
}

func (h *senderRewriteHandlers) list(ctx context.Context, in *listSenderRewriteRulesInput) (*listSenderRewriteRulesOutput, error) {
	var f cp.SenderRewriteFilter
	if in.Scope != "" {
		f.Scope = ptr(cp.SenderRewriteScope(in.Scope))
	}
	if in.ScopeID != "" {
		id, err := parseIDPtr("scopeId", &in.ScopeID)
		if err != nil {
			return nil, err
		}
		f.ScopeID = id
	}
	rules, err := h.store.List(ctx, f)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listSenderRewriteRulesOutput{Body: make([]senderRewriteRuleDTO, 0, len(rules))}
	for _, r := range rules {
		out.Body = append(out.Body, toSenderRewriteRuleDTO(r))
	}
	return out, nil
}

type createSenderRewriteRuleInput struct{ Body senderRewriteRuleCreateBody }
type senderRewriteRuleOutput struct{ Body senderRewriteRuleDTO }

func (h *senderRewriteHandlers) create(ctx context.Context, in *createSenderRewriteRuleInput) (*senderRewriteRuleOutput, error) {
	b := in.Body
	if b.Direction == "mo" {
		return nil, humaerr.FailValidation("invalid direction",
			humaerr.FieldError{Field: "direction", Message: "only mt rules are evaluated"})
	}
	scope := cp.SenderRewriteScope(b.Scope)
	scopeID, err := parseIDPtr("scope_id", b.ScopeID)
	if err != nil {
		return nil, err
	}
	if (scope == cp.RewriteScopePlatform) != (scopeID == nil) {
		return nil, humaerr.FailValidation("invalid scope",
			humaerr.FieldError{Field: "scope_id", Message: "must be null for the platform scope and set for any other"})
	}
	charset, err := encodeCharset(b.SanitizeCharset)
	if err != nil {
		return nil, err
	}
	rule := cp.NewSenderRewriteRule{
		Scope:              scope,
		ScopeID:            scopeID,
		MatchSenderPattern: b.MatchSenderPattern,
		MatchDestPattern:   b.MatchDestPattern,
		RewriteType:        cp.SenderRewriteType(b.RewriteType),
		RewriteTo:          b.RewriteTo,
		FallbackPool:       b.FallbackPool,
		MaxLength:          b.MaxLength,
		SanitizeCharset:    charset,
		Priority:           *b.Priority,
		Reason:             b.Reason,
	}
	if err := validateRewrite(cp.SenderRewriteRule{
		MatchSenderPattern: rule.MatchSenderPattern, MatchDestPattern: rule.MatchDestPattern,
		RewriteType: rule.RewriteType, RewriteTo: rule.RewriteTo, FallbackPool: rule.FallbackPool,
		MaxLength: rule.MaxLength,
	}); err != nil {
		return nil, err
	}
	r, err := h.store.Create(ctx, rule)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &senderRewriteRuleOutput{Body: toSenderRewriteRuleDTO(r)}, nil
}

type updateSenderRewriteRuleInput struct {
	ID   string `path:"id" format:"uuid"`
	Body senderRewriteRuleUpdateBody
}

func (h *senderRewriteHandlers) update(ctx context.Context, in *updateSenderRewriteRuleInput) (*senderRewriteRuleOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("sender rewrite rule")
	}
	b := in.Body
	charset, err := encodeCharset(b.SanitizeCharset)
	if err != nil {
		return nil, err
	}
	patch := cp.SenderRewriteRulePatch{
		MatchSenderPattern: b.MatchSenderPattern,
		MatchDestPattern:   b.MatchDestPattern,
		RewriteType:        enumPtr[cp.SenderRewriteType](b.RewriteType),
		RewriteTo:          b.RewriteTo,
		FallbackPool:       b.FallbackPool,
		MaxLength:          b.MaxLength,
		SanitizeCharset:    charset,
		Priority:           b.Priority,
		Reason:             b.Reason,
		Status:             b.Status,
	}

	// ponytail: Get then Update without a lock — two concurrent PATCHes can each pass alone and
	// together leave a rule the engine cannot apply (a blank rewrite_to on a static rule). SELECT … FOR
	// UPDATE in one transaction if concurrent edits of one rule become real.
	current, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if err := validateRewrite(mergeRewrite(current, patch)); err != nil {
		return nil, err
	}

	r, err := h.store.Update(ctx, id, patch)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &senderRewriteRuleOutput{Body: toSenderRewriteRuleDTO(r)}, nil
}

type senderRewriteRuleIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *senderRewriteHandlers) delete(ctx context.Context, in *senderRewriteRuleIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("sender rewrite rule")
	}
	if err := h.store.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

// mergeRewrite applies p to r the way the repository's COALESCE update will. Only the fields
// validateRewrite reads are merged.
func mergeRewrite(r cp.SenderRewriteRule, p cp.SenderRewriteRulePatch) cp.SenderRewriteRule {
	if p.MatchSenderPattern != nil {
		r.MatchSenderPattern = p.MatchSenderPattern
	}
	if p.MatchDestPattern != nil {
		r.MatchDestPattern = p.MatchDestPattern
	}
	if p.RewriteType != nil {
		r.RewriteType = *p.RewriteType
	}
	if p.RewriteTo != nil {
		r.RewriteTo = p.RewriteTo
	}
	if p.FallbackPool != nil {
		r.FallbackPool = p.FallbackPool
	}
	if p.MaxLength != nil {
		r.MaxLength = p.MaxLength
	}
	return r
}

// validateRewrite refuses a rule the evaluation engine could not apply: an uncompilable pattern, or a
// rewrite type missing the field it reads. Fields another type reads are ignored, not refused: the
// COALESCE update cannot clear them, so refusing them would forbid every change of type. A sanitize
// rule needs nothing — encodeCharset has checked a supplied charset, and null keeps the default.
func validateRewrite(r cp.SenderRewriteRule) error {
	for _, f := range []struct {
		name string
		p    *string
	}{{"match_sender_pattern", r.MatchSenderPattern}, {"match_dest_pattern", r.MatchDestPattern}} {
		if f.p == nil {
			continue
		}
		if _, err := regexp.Compile(*f.p); err != nil {
			return humaerr.FailValidation("invalid pattern", humaerr.FieldError{Field: f.name, Message: err.Error()})
		}
	}
	missing := func(field, msg string) error {
		return humaerr.FailValidation("invalid rule", humaerr.FieldError{Field: field, Message: msg})
	}
	switch r.RewriteType {
	case cp.RewriteStatic:
		if r.RewriteTo == nil || strings.TrimSpace(*r.RewriteTo) == "" {
			return missing("rewrite_to", "is required for a static rule")
		}
	case cp.RewriteFallbackPool:
		if len(r.FallbackPool) == 0 {
			return missing("fallback_pool_json", "must list at least one sender ID")
		}
		for _, s := range r.FallbackPool {
			if strings.TrimSpace(s) == "" {
				return missing("fallback_pool_json", "must not contain a blank sender ID")
			}
		}
	case cp.RewriteTruncate:
		if r.MaxLength == nil {
			return missing("max_length", "is required for a truncate rule")
		}
	}
	return nil
}

// encodeCharset validates a supplied sanitize charset and returns the jsonb it is stored as. Only
// {"allowed": "<non-empty string>"} is accepted: a misspelt key would otherwise fall back to the
// default charset without anyone noticing.
func encodeCharset(m map[string]any) (json.RawMessage, error) {
	if m == nil {
		return nil, nil
	}
	allowed, ok := m["allowed"].(string)
	if len(m) != 1 || !ok || allowed == "" {
		return nil, humaerr.FailValidation("invalid sanitize charset", humaerr.FieldError{
			Field: "sanitize_charset_json", Message: `must be {"allowed": "<characters kept>"} with a non-empty string`,
		})
	}
	raw, _ := json.Marshal(map[string]string{"allowed": allowed})
	return raw, nil
}

type testSenderRewriteRuleInput struct {
	ID   string `path:"id" format:"uuid"`
	Body struct {
		SourceAddr string  `json:"source_addr" doc:"The sender as the client submits it."`
		DestAddr   string  `json:"dest_addr" doc:"The destination as the pool sees it: digits only, no +."`
		MessageID  *string `json:"message_id,omitempty" format:"uuid" doc:"A fallback_pool rule picks its sender from the message id; give a CDR's to reproduce its choice. Absent means the nil UUID."`
	}
}

type testSenderRewriteRuleOutput struct {
	Body struct {
		Matched         bool    `json:"matched"`
		RewrittenSource *string `json:"rewritten_source,omitempty" nullable:"true"`
	}
}

// test runs one rule through senderrewrite.EvalRule, the function the pool's snapshot applies, so the
// answer is what the pool would send for that sample.
func (h *senderRewriteHandlers) test(ctx context.Context, in *testSenderRewriteRuleInput) (*testSenderRewriteRuleOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("sender rewrite rule")
	}
	messageID, err := parseIDPtr("message_id", in.Body.MessageID)
	if err != nil {
		return nil, err
	}
	if messageID == nil {
		messageID = &uuid.Nil
	}
	rule, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	rewritten, matched, err := senderrewrite.EvalRule(rule, in.Body.SourceAddr, in.Body.DestAddr, *messageID)
	if err != nil {
		return nil, humaerr.Fail(errs.ErrValidation, "the stored rule has a pattern that does not compile: %v", err)
	}
	out := &testSenderRewriteRuleOutput{}
	out.Body.Matched = matched
	if matched {
		out.Body.RewrittenSource = &rewritten
	}
	return out, nil
}
