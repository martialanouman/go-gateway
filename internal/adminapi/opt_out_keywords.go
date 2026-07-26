package adminapi

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// optOutKeywordDTO is the wire form of an OptOutKeyword (contract schema OptOutKeyword).
type optOutKeywordDTO struct {
	ID                string  `json:"id" format:"uuid"`
	CountryCode       *string `json:"country_code,omitempty" nullable:"true"`
	Keyword           string  `json:"keyword"`
	Action            string  `json:"action" enum:"suppress,unsuppress,help"`
	MatchType         string  `json:"match_type" enum:"exact,prefix"`
	AutoReplyTemplate *string `json:"auto_reply_template,omitempty" nullable:"true"`
	Status            string  `json:"status" enum:"active,disabled"`
}

func toOptOutKeywordDTO(k cp.OptOutKeyword) optOutKeywordDTO {
	return optOutKeywordDTO{
		ID:                idString(k.ID),
		CountryCode:       k.CountryCode,
		Keyword:           k.Keyword,
		Action:            string(k.Action),
		MatchType:         string(k.MatchType),
		AutoReplyTemplate: k.AutoReplyTemplate,
		Status:            string(k.Status),
	}
}

type optOutKeywordCreateBody struct {
	CountryCode       *string `json:"country_code,omitempty" nullable:"true"`
	Keyword           string  `json:"keyword"`
	Action            string  `json:"action" enum:"suppress,unsuppress,help"`
	MatchType         *string `json:"match_type,omitempty" enum:"exact,prefix"`
	AutoReplyTemplate *string `json:"auto_reply_template,omitempty" nullable:"true"`
}

type optOutKeywordUpdateBody struct {
	Keyword           *string `json:"keyword,omitempty"`
	Action            *string `json:"action,omitempty" enum:"suppress,unsuppress,help"`
	MatchType         *string `json:"match_type,omitempty" enum:"exact,prefix"`
	AutoReplyTemplate *string `json:"auto_reply_template,omitempty" nullable:"true"`
	Status            *string `json:"status,omitempty" enum:"active,disabled"`
}

// OptOutKeywordStore is the persistence the opt-out keyword handlers need (declared consumer-side).
// *postgres.OptOutKeywordRepo satisfies it.
type OptOutKeywordStore interface {
	List(ctx context.Context) ([]cp.OptOutKeyword, error)
	Create(ctx context.Context, in cp.NewOptOutKeyword) (cp.OptOutKeyword, error)
	Update(ctx context.Context, id uuid.UUID, p cp.OptOutKeywordPatch) (cp.OptOutKeyword, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

type optOutKeywordHandlers struct {
	store OptOutKeywordStore
}

func registerOptOutKeywords(api huma.API, store OptOutKeywordStore) {
	h := &optOutKeywordHandlers{store: store}

	register(api, huma.Operation{
		OperationID: "list-opt-out-keywords", Method: http.MethodGet, Path: "/admin/opt-out-keywords",
		Summary: "List opt-out keywords", Tags: []string{"Opt-out Keywords"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-opt-out-keyword", Method: http.MethodPost, Path: "/admin/opt-out-keywords",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create an opt-out keyword", Tags: []string{"Opt-out Keywords"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusConflict},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "update-opt-out-keyword", Method: http.MethodPatch, Path: "/admin/opt-out-keywords/{id}",
		Summary: "Update an opt-out keyword", Tags: []string{"Opt-out Keywords"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-opt-out-keyword", Method: http.MethodDelete, Path: "/admin/opt-out-keywords/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete an opt-out keyword", Tags: []string{"Opt-out Keywords"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)
}

type listOptOutKeywordsOutput struct {
	Body []optOutKeywordDTO
}

func (h *optOutKeywordHandlers) list(ctx context.Context, _ *struct{}) (*listOptOutKeywordsOutput, error) {
	kws, err := h.store.List(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listOptOutKeywordsOutput{Body: make([]optOutKeywordDTO, 0, len(kws))}
	for _, k := range kws {
		out.Body = append(out.Body, toOptOutKeywordDTO(k))
	}
	return out, nil
}

type createOptOutKeywordInput struct{ Body optOutKeywordCreateBody }
type optOutKeywordOutput struct{ Body optOutKeywordDTO }

func (h *optOutKeywordHandlers) create(ctx context.Context, in *createOptOutKeywordInput) (*optOutKeywordOutput, error) {
	k, err := h.store.Create(ctx, cp.NewOptOutKeyword{
		CountryCode:       in.Body.CountryCode,
		Keyword:           in.Body.Keyword,
		Action:            cp.OptOutAction(in.Body.Action),
		MatchType:         enumPtr[cp.OptOutMatchType](in.Body.MatchType),
		AutoReplyTemplate: in.Body.AutoReplyTemplate,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &optOutKeywordOutput{Body: toOptOutKeywordDTO(k)}, nil
}

type updateOptOutKeywordInput struct {
	ID   string `path:"id" format:"uuid"`
	Body optOutKeywordUpdateBody
}

func (h *optOutKeywordHandlers) update(ctx context.Context, in *updateOptOutKeywordInput) (*optOutKeywordOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("opt-out keyword")
	}
	k, err := h.store.Update(ctx, id, cp.OptOutKeywordPatch{
		Keyword:           in.Body.Keyword,
		Action:            enumPtr[cp.OptOutAction](in.Body.Action),
		MatchType:         enumPtr[cp.OptOutMatchType](in.Body.MatchType),
		AutoReplyTemplate: in.Body.AutoReplyTemplate,
		Status:            enumPtr[cp.OptOutKeywordStatus](in.Body.Status),
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &optOutKeywordOutput{Body: toOptOutKeywordDTO(k)}, nil
}

type optOutKeywordIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *optOutKeywordHandlers) delete(ctx context.Context, in *optOutKeywordIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("opt-out keyword")
	}
	if err := h.store.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}
