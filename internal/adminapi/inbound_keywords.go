package adminapi

import (
	"context"
	"math"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// inboundKeywordDTO is the wire form of an InboundKeyword (contract schema InboundKeyword). Required
// and non-null: id, inbound_number_id, keyword, match_type, account_id, status. priority is present
// but not required, so it is a pointer with omitempty — always populated here, never omitted in
// practice, but kept out of the required set to match the contract. The contract InboundKeyword
// carries no timestamps, so created_at/updated_at are not exposed (unlike InboundNumber).
type inboundKeywordDTO struct {
	ID              string `json:"id" format:"uuid"`
	InboundNumberID string `json:"inbound_number_id" format:"uuid"`
	Keyword         string `json:"keyword"`
	MatchType       string `json:"match_type" enum:"exact,prefix,regex"`
	AccountID       string `json:"account_id" format:"uuid"`
	Priority        *int   `json:"priority,omitempty"`
	Status          string `json:"status" enum:"active,disabled"`
}

func toInboundKeywordDTO(k cp.InboundKeyword) inboundKeywordDTO {
	return inboundKeywordDTO{
		ID:              idString(k.ID),
		InboundNumberID: idString(k.InboundNumberID),
		Keyword:         k.Keyword,
		MatchType:       string(k.MatchType),
		AccountID:       idString(k.AccountID),
		Priority:        ptr(k.Priority),
		Status:          string(k.Status),
	}
}

// inboundKeywordCreateBody is the request body of create-inbound-keyword (contract
// InboundKeywordCreate). keyword and account_id are required; match_type (DDL default 'prefix') and
// priority (DDL default 0) are optional and defaulted by the handler.
type inboundKeywordCreateBody struct {
	Keyword   string  `json:"keyword"`
	MatchType *string `json:"match_type,omitempty" enum:"exact,prefix,regex"`
	AccountID string  `json:"account_id" format:"uuid"`
	Priority  *int    `json:"priority,omitempty"`
}

// inboundKeywordUpdateBody is the request body of update-inbound-keyword (contract
// InboundKeywordUpdate). Every field is optional; a nil field is left unchanged.
type inboundKeywordUpdateBody struct {
	Keyword   *string `json:"keyword,omitempty"`
	MatchType *string `json:"match_type,omitempty" enum:"exact,prefix,regex"`
	AccountID *string `json:"account_id,omitempty" format:"uuid"`
	Priority  *int    `json:"priority,omitempty"`
	Status    *string `json:"status,omitempty" enum:"active,disabled"`
}

type inboundKeywordHandlers struct {
	store InboundKeywordStore
}

func registerInboundKeywords(api huma.API, store InboundKeywordStore) {
	h := &inboundKeywordHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-inbound-keywords", Method: http.MethodGet,
		Path:    "/admin/inbound-numbers/{id}/keywords",
		Summary: "List keywords for a shared inbound number", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminRead),
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-inbound-keyword", Method: http.MethodPost,
		Path:          "/admin/inbound-numbers/{id}/keywords",
		DefaultStatus: http.StatusCreated,
		Summary:       "Add a keyword mapping", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
	}, h.create)

	register(api, huma.Operation{
		OperationID: "update-inbound-keyword", Method: http.MethodPatch,
		Path:    "/admin/inbound-numbers/{id}/keywords/{keywordId}",
		Summary: "Update a keyword mapping", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusNotFound},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-inbound-keyword", Method: http.MethodDelete,
		Path:          "/admin/inbound-numbers/{id}/keywords/{keywordId}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a keyword mapping", Tags: []string{"Inbound Numbers"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusNotFound},
	}, h.delete)
}

type listInboundKeywordsInput struct {
	ID string `path:"id" format:"uuid"`
}

type listInboundKeywordsOutput struct {
	Body []inboundKeywordDTO
}

func (h *inboundKeywordHandlers) list(ctx context.Context, in *listInboundKeywordsInput) (*listInboundKeywordsOutput, error) {
	out := &listInboundKeywordsOutput{Body: make([]inboundKeywordDTO, 0)}
	numberID, err := uuid.Parse(in.ID)
	if err != nil {
		return out, nil
	}
	keywords, err := h.store.ListByInboundNumber(ctx, numberID)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	for _, k := range keywords {
		out.Body = append(out.Body, toInboundKeywordDTO(k))
	}
	return out, nil
}

type createInboundKeywordInput struct {
	ID   string `path:"id" format:"uuid"`
	Body inboundKeywordCreateBody
}

type inboundKeywordOutput struct{ Body inboundKeywordDTO }

func (h *inboundKeywordHandlers) create(ctx context.Context, in *createInboundKeywordInput) (*inboundKeywordOutput, error) {
	numberID, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("inbound number")
	}
	accountID, err := uuid.Parse(in.Body.AccountID)
	if err != nil {
		return nil, huma.Error422UnprocessableEntity("account_id is not a valid UUID")
	}
	if err := checkPriorityRange(in.Body.Priority); err != nil {
		return nil, err
	}
	// match_type and priority default to the DDL values when the request omits them, so the row is
	// always inserted with non-null columns.
	matchType := cp.MatchPrefix
	if in.Body.MatchType != nil {
		matchType = cp.MatchType(*in.Body.MatchType)
	}
	priority := 0
	if in.Body.Priority != nil {
		priority = *in.Body.Priority
	}
	k, err := h.store.Create(ctx, cp.NewInboundKeyword{
		InboundNumberID: numberID,
		Keyword:         in.Body.Keyword,
		MatchType:       matchType,
		AccountID:       accountID,
		Priority:        priority,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &inboundKeywordOutput{Body: toInboundKeywordDTO(k)}, nil
}

type updateInboundKeywordInput struct {
	ID        string `path:"id" format:"uuid"`
	KeywordID string `path:"keywordId" format:"uuid"`
	Body      inboundKeywordUpdateBody
}

func (h *inboundKeywordHandlers) update(ctx context.Context, in *updateInboundKeywordInput) (*inboundKeywordOutput, error) {
	numberID, keywordID, err := parseKeywordPath(in.ID, in.KeywordID)
	if err != nil {
		return nil, err
	}
	if err := checkPriorityRange(in.Body.Priority); err != nil {
		return nil, err
	}
	accountID, err := parseIDPtr("account_id", in.Body.AccountID)
	if err != nil {
		return nil, err
	}
	k, err := h.store.Update(ctx, numberID, keywordID, cp.InboundKeywordPatch{
		Keyword:   in.Body.Keyword,
		MatchType: enumPtr[cp.MatchType](in.Body.MatchType),
		AccountID: accountID,
		Priority:  in.Body.Priority,
		Status:    enumPtr[cp.InboundKeywordStatus](in.Body.Status),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &inboundKeywordOutput{Body: toInboundKeywordDTO(k)}, nil
}

type deleteInboundKeywordInput struct {
	ID        string `path:"id" format:"uuid"`
	KeywordID string `path:"keywordId" format:"uuid"`
}

func (h *inboundKeywordHandlers) delete(ctx context.Context, in *deleteInboundKeywordInput) (*deleteOutput, error) {
	numberID, keywordID, err := parseKeywordPath(in.ID, in.KeywordID)
	if err != nil {
		return nil, err
	}
	if err := h.store.Delete(ctx, numberID, keywordID); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

// checkPriorityRange rejects a priority outside the int32 range the DDL column holds. The contract
// types priority as a bare integer with no bounds, and Go decodes it into a 64-bit int, so without
// this guard a value above math.MaxInt32 would wrap silently on the way to the int32 column and
// corrupt the MO evaluation order. A nil (omitted) priority is left to its DDL default.
func checkPriorityRange(priority *int) error {
	if priority != nil && (*priority < math.MinInt32 || *priority > math.MaxInt32) {
		return huma.Error422UnprocessableEntity("priority is out of range")
	}
	return nil
}

// parseKeywordPath parses the number and keyword path ids, reporting a not-found (never a 500) for a
// malformed id — an unparseable id names nothing.
func parseKeywordPath(numberID, keywordID string) (uuid.UUID, uuid.UUID, error) {
	n, err := uuid.Parse(numberID)
	if err != nil {
		return uuid.Nil, uuid.Nil, notFound("inbound number")
	}
	k, err := uuid.Parse(keywordID)
	if err != nil {
		return uuid.Nil, uuid.Nil, notFound("inbound keyword")
	}
	return n, k, nil
}
