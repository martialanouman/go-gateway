package adminapi

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// suppressionDTO is the wire form of a Suppression (contract schema Suppression).
type suppressionDTO struct {
	ID        string    `json:"id" format:"uuid"`
	Scope     string    `json:"scope" enum:"inbound_number,smpp_account,customer,platform"`
	ScopeID   *string   `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	MSISDN    string    `json:"msisdn"`
	Source    string    `json:"source" enum:"mo_stop,admin,import,carrier,regulator"`
	Reason    *string   `json:"reason,omitempty" nullable:"true"`
	CreatedAt time.Time `json:"created_at" format:"date-time"`
}

func toSuppressionDTO(s cp.Suppression) suppressionDTO {
	return suppressionDTO{
		ID:        idString(s.ID),
		Scope:     string(s.Scope),
		ScopeID:   idPtr(s.ScopeID),
		MSISDN:    s.MSISDN,
		Source:    string(s.Source),
		Reason:    s.Reason,
		CreatedAt: s.CreatedAt,
	}
}

type suppressionCreateBody struct {
	Scope   string  `json:"scope" enum:"inbound_number,smpp_account,customer,platform"`
	ScopeID *string `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	MSISDN  string  `json:"msisdn"`
	Source  string  `json:"source" enum:"mo_stop,admin,import,carrier,regulator"`
	Reason  *string `json:"reason,omitempty" nullable:"true"`
}

type suppressionPage struct {
	PageMeta
	Data []suppressionDTO `json:"data"`
}

type importSuppressionsBody struct {
	Scope   string   `json:"scope" enum:"inbound_number,smpp_account,customer,platform"`
	ScopeID *string  `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
	Source  *string  `json:"source,omitempty" enum:"mo_stop,admin,import,carrier,regulator"`
	MSISDNs []string `json:"msisdns" maxItems:"10000"`
}

type checkSuppressionBody struct {
	MSISDN     string  `json:"msisdn"`
	SenderAddr *string `json:"sender_addr,omitempty" nullable:"true"`
	AccountID  *string `json:"account_id,omitempty" format:"uuid" nullable:"true"`
}

type matchedScopeDTO struct {
	Scope   string  `json:"scope" enum:"inbound_number,smpp_account,customer,platform"`
	ScopeID *string `json:"scope_id,omitempty" format:"uuid" nullable:"true"`
}

type suppressionCheckResultDTO struct {
	Blocked       bool              `json:"blocked"`
	MatchedScopes []matchedScopeDTO `json:"matched_scopes"`
}

// asyncJobDTO is the wire form of AsyncJob. The M5 import runs synchronously, so the job it returns is
// already completed — a real background queue is a later concern.
type asyncJobDTO struct {
	JobID      string     `json:"job_id" format:"uuid"`
	Status     string     `json:"status" enum:"queued,running,completed,failed"`
	Progress   *float64   `json:"progress,omitempty" nullable:"true" minimum:"0" maximum:"1"`
	CreatedAt  time.Time  `json:"created_at" format:"date-time"`
	FinishedAt *time.Time `json:"finished_at,omitempty" format:"date-time" nullable:"true"`
	Detail     *string    `json:"detail,omitempty" nullable:"true"`
}

// SuppressionAdminStore is the persistence the suppression handlers need (declared consumer-side).
// *postgres.SuppressionRepo satisfies it.
type SuppressionAdminStore interface {
	CreateReturning(ctx context.Context, in cp.NewSuppression) (cp.Suppression, error)
	ListPage(ctx context.Context, f cp.SuppressionFilter) (cp.Page[cp.Suppression], error)
	DeleteByID(ctx context.Context, id uuid.UUID) error
	Import(ctx context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, source cp.SuppressionSource, msisdns []string) (int64, error)
	IsSuppressed(ctx context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) (bool, error)
}

// accountCustomerLookup resolves an account to its owning customer, for the check's customer scope.
type accountCustomerLookup interface {
	Get(ctx context.Context, id uuid.UUID) (cp.Account, error)
}

// inboundNumberLookup lists inbound numbers, to resolve a sender address to its inbound_number scope.
type inboundNumberLookup interface {
	List(ctx context.Context) ([]cp.InboundNumber, error)
}

type suppressionHandlers struct {
	store    SuppressionAdminStore
	accounts accountCustomerLookup
	inbound  inboundNumberLookup
}

func registerSuppressions(api huma.API, store SuppressionAdminStore, accounts accountCustomerLookup, inbound inboundNumberLookup) {
	h := &suppressionHandlers{store: store, accounts: accounts, inbound: inbound}

	register(api, huma.Operation{
		OperationID: "list-suppressions", Method: http.MethodGet, Path: "/admin/suppressions",
		Summary: "List suppressions", Tags: []string{"Suppression"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-suppression", Method: http.MethodPost, Path: "/admin/suppressions",
		DefaultStatus: http.StatusCreated,
		Summary:       "Add a suppression", Tags: []string{"Suppression"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusConflict},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "import-suppressions", Method: http.MethodPost, Path: "/admin/suppressions/import",
		DefaultStatus: http.StatusAccepted,
		Summary:       "Bulk import suppressions (async)", Tags: []string{"Suppression"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.importSuppressions)

	register(api, huma.Operation{
		OperationID: "check-suppression", Method: http.MethodPost, Path: "/admin/suppressions/check",
		Summary: "Would this recipient be blocked, and by which scope?", Tags: []string{"Suppression"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.check)

	register(api, huma.Operation{
		OperationID: "delete-suppression", Method: http.MethodDelete, Path: "/admin/suppressions/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Un-suppress (audit-logged)", Tags: []string{"Suppression"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)
}

type listSuppressionsInput struct {
	Scope   string `query:"scope" enum:"inbound_number,smpp_account,customer,platform" doc:"Filter by scope."`
	ScopeID string `query:"scopeId" format:"uuid" doc:"Filter by scope entity id."`
	MSISDN  string `query:"msisdn" doc:"Filter by exact recipient MSISDN."`
	Cursor  string `query:"cursor" doc:"Opaque page position from a previous page."`
	Limit   int    `query:"limit" minimum:"1" maximum:"500" default:"50" doc:"Page size."`
}

type listSuppressionsOutput struct{ Body suppressionPage }

func (h *suppressionHandlers) list(ctx context.Context, in *listSuppressionsInput) (*listSuppressionsOutput, error) {
	reveal := mayRevealMSISDN(ctx)
	filter := cp.SuppressionFilter{Limit: in.Limit}
	if in.Scope != "" {
		s := cp.SuppressionScope(in.Scope)
		filter.Scope = &s
	}
	if in.ScopeID != "" {
		id, err := uuid.Parse(in.ScopeID)
		if err != nil {
			return nil, humaerr.FailValidation("invalid scopeId",
				humaerr.FieldError{Field: "scopeId", Message: "must be a UUID"})
		}
		filter.ScopeID = &id
	}
	if in.MSISDN != "" {
		// Suppressions are stored in the canonical E.164 form, so the filter must be normalized too —
		// a raw "+225…" or national spelling would never match the stored "225…".
		msisdn, err := normalizeMSISDN(in.MSISDN)
		if err != nil {
			return nil, err
		}
		filter.MSISDN = &msisdn
	}
	after, err := cp.DecodeCursor(cp.Cursor(in.Cursor))
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	filter.After = after

	page, err := h.store.ListPage(ctx, filter)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listSuppressionsOutput{}
	out.Body.NextCursor = cursorString(string(page.NextCursor))
	out.Body.HasMore = page.HasMore
	out.Body.Data = make([]suppressionDTO, 0, len(page.Items))
	for _, s := range page.Items {
		dto := toSuppressionDTO(s)
		// A bulk list of suppressed numbers is the widest subscriber-data read in the Admin API; masking the
		// trace while leaving this in clear would be a door beside the wall. create/check echo a number the
		// caller supplied, so they are left alone.
		dto.MSISDN = maskMSISDN(dto.MSISDN, reveal)
		out.Body.Data = append(out.Body.Data, dto)
	}
	return out, nil
}

type createSuppressionInput struct{ Body suppressionCreateBody }
type suppressionOutput struct{ Body suppressionDTO }

func (h *suppressionHandlers) create(ctx context.Context, in *createSuppressionInput) (*suppressionOutput, error) {
	scopeID, err := parseIDPtr("scope_id", in.Body.ScopeID)
	if err != nil {
		return nil, err
	}
	msisdn, err := normalizeMSISDN(in.Body.MSISDN)
	if err != nil {
		return nil, err
	}
	s, err := h.store.CreateReturning(ctx, cp.NewSuppression{
		Scope:   cp.SuppressionScope(in.Body.Scope),
		ScopeID: scopeID,
		MSISDN:  msisdn,
		Source:  cp.SuppressionSource(in.Body.Source),
		Reason:  in.Body.Reason,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &suppressionOutput{Body: toSuppressionDTO(s)}, nil
}

type suppressionIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *suppressionHandlers) delete(ctx context.Context, in *suppressionIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("suppression")
	}
	if err := h.store.DeleteByID(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

type importSuppressionsInput struct{ Body importSuppressionsBody }
type importSuppressionsOutput struct{ Body asyncJobDTO }

func (h *suppressionHandlers) importSuppressions(ctx context.Context, in *importSuppressionsInput) (*importSuppressionsOutput, error) {
	scopeID, err := parseIDPtr("scope_id", in.Body.ScopeID)
	if err != nil {
		return nil, err
	}
	source := cp.SuppressionSourceImport
	if in.Body.Source != nil {
		source = cp.SuppressionSource(*in.Body.Source)
	}
	// Normalize every MSISDN up front: a non-canonical value would fail the whole batch at the CHECK,
	// so reject it here with a clear field error instead.
	normalized := make([]string, 0, len(in.Body.MSISDNs))
	for _, raw := range in.Body.MSISDNs {
		n, nerr := e164.Normalize(raw)
		if nerr != nil {
			return nil, humaerr.FailValidation("invalid msisdn",
				humaerr.FieldError{Field: "msisdns", Message: "entry is not a valid E.164 number"})
		}
		normalized = append(normalized, n)
	}

	inserted, err := h.store.Import(ctx, cp.SuppressionScope(in.Body.Scope), scopeID, source, normalized)
	if err != nil {
		return nil, humaerr.FromError(err)
	}

	// The M5 import is synchronous, so the job is already done. now() is derived per-request from the
	// server clock, not a fixed value.
	now := time.Now().UTC()
	detail := formatImportDetail(inserted, len(normalized))
	return &importSuppressionsOutput{Body: asyncJobDTO{
		JobID:      uuid.NewString(),
		Status:     "completed",
		Progress:   ptr(1.0),
		CreatedAt:  now,
		FinishedAt: &now,
		Detail:     &detail,
	}}, nil
}

type checkSuppressionInput struct{ Body checkSuppressionBody }
type checkSuppressionOutput struct{ Body suppressionCheckResultDTO }

func (h *suppressionHandlers) check(ctx context.Context, in *checkSuppressionInput) (*checkSuppressionOutput, error) {
	msisdn, err := normalizeMSISDN(in.Body.MSISDN)
	if err != nil {
		return nil, err
	}
	accountID, err := parseIDPtr("account_id", in.Body.AccountID)
	if err != nil {
		return nil, err
	}

	matched := make([]matchedScopeDTO, 0, 4)
	add := func(scope cp.SuppressionScope, scopeID *uuid.UUID) {
		matched = append(matched, matchedScopeDTO{Scope: string(scope), ScopeID: idPtr(scopeID)})
	}

	// Exact (database-backed) checks per applicable scope, so a just-added suppression is reflected
	// immediately — the admin diagnostic must not depend on the router's cold Bloom snapshot.
	if ok, cerr := h.store.IsSuppressed(ctx, cp.SuppressionScopePlatform, nil, msisdn); cerr != nil {
		return nil, humaerr.FromError(cerr)
	} else if ok {
		add(cp.SuppressionScopePlatform, nil)
	}

	if accountID != nil {
		if ok, cerr := h.store.IsSuppressed(ctx, cp.SuppressionScopeAccount, accountID, msisdn); cerr != nil {
			return nil, humaerr.FromError(cerr)
		} else if ok {
			add(cp.SuppressionScopeAccount, accountID)
		}
		if custID, ok := h.customerOf(ctx, *accountID); ok {
			if sup, cerr := h.store.IsSuppressed(ctx, cp.SuppressionScopeCustomer, &custID, msisdn); cerr != nil {
				return nil, humaerr.FromError(cerr)
			} else if sup {
				add(cp.SuppressionScopeCustomer, &custID)
			}
		}
	}

	if in.Body.SenderAddr != nil {
		if inbID, ok, cerr := h.inboundOf(ctx, *in.Body.SenderAddr); cerr != nil {
			return nil, humaerr.FromError(cerr)
		} else if ok {
			if sup, serr := h.store.IsSuppressed(ctx, cp.SuppressionScopeInboundNumber, &inbID, msisdn); serr != nil {
				return nil, humaerr.FromError(serr)
			} else if sup {
				add(cp.SuppressionScopeInboundNumber, &inbID)
			}
		}
	}

	return &checkSuppressionOutput{Body: suppressionCheckResultDTO{Blocked: len(matched) > 0, MatchedScopes: matched}}, nil
}

// customerOf resolves an account's owning customer. An unknown account yields ok=false (the customer
// scope simply does not apply), never an error — the check is a best-effort diagnostic.
func (h *suppressionHandlers) customerOf(ctx context.Context, accountID uuid.UUID) (uuid.UUID, bool) {
	acct, err := h.accounts.Get(ctx, accountID)
	if err != nil {
		return uuid.Nil, false
	}
	return acct.CustomerID, true
}

// inboundOf resolves a sender address to the inbound number it is, using the same normalized-address
// convention as the MO router and the opt-out enforcer.
func (h *suppressionHandlers) inboundOf(ctx context.Context, senderAddr string) (uuid.UUID, bool, error) {
	nums, err := h.inbound.List(ctx)
	if err != nil {
		return uuid.Nil, false, err
	}
	key := e164.NormalizeAddr(senderAddr)
	for _, n := range nums {
		if e164.NormalizeAddr(n.Address) == key {
			return n.ID, true, nil
		}
	}
	return uuid.Nil, false, nil
}

// normalizeMSISDN canonicalizes a recipient MSISDN for a suppression write/lookup, matching the
// schema's canonical form. An invalid number is a 422 naming the field.
func normalizeMSISDN(raw string) (string, error) {
	n, err := e164.Normalize(raw)
	if err != nil {
		return "", humaerr.FailValidation("invalid msisdn",
			humaerr.FieldError{Field: "msisdn", Message: "must be a valid E.164 number"})
	}
	return n, nil
}

func formatImportDetail(inserted int64, total int) string {
	return "imported " + strconv.FormatInt(inserted, 10) + " of " + strconv.Itoa(total) + " (duplicates skipped)"
}
