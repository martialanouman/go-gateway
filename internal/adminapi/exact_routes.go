package adminapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/platform/async"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
)

// exactRouteDTO is the wire form of an exact route (contract schema ExactRoute).
type exactRouteDTO struct {
	MSISDN     string     `json:"msisdn"`
	TargetType string     `json:"target_type" enum:"connector,route"`
	TargetID   string     `json:"target_id" format:"uuid"`
	Source     string     `json:"source" enum:"mnp_import,manual,carrier_feed"`
	ImportedAt *time.Time `json:"imported_at,omitempty" format:"date-time" nullable:"true"`
	UpdatedAt  time.Time  `json:"updated_at" format:"date-time"`
}

func toExactRouteDTO(r exact.Route) exactRouteDTO {
	return exactRouteDTO{
		MSISDN:     r.MSISDN,
		TargetType: string(r.Target.Type),
		TargetID:   r.Target.ID.String(),
		Source:     string(r.Source),
		ImportedAt: r.ImportedAt,
		UpdatedAt:  r.UpdatedAt,
	}
}

type exactRouteCreateBody struct {
	MSISDN     string `json:"msisdn"`
	TargetType string `json:"target_type" enum:"connector,route"`
	TargetID   string `json:"target_id" format:"uuid"`
	Source     string `json:"source,omitempty" enum:"mnp_import,manual,carrier_feed" default:"manual"`
}

type exactRouteUpdateBody struct {
	TargetType *string `json:"target_type,omitempty" enum:"connector,route" nullable:"false"`
	TargetID   *string `json:"target_id,omitempty" format:"uuid" nullable:"false"`
	Source     *string `json:"source,omitempty" enum:"mnp_import,manual,carrier_feed" nullable:"false"`
}

type exactRoutePage struct {
	PageMeta
	Data []exactRouteDTO `json:"data"`
}

// ExactRouteAdminStore is the persistence the exact-route handlers need (declared consumer-side).
// *postgres.ExactRouteRepo satisfies it. List is keyset-paginated by msisdn (the primary key), so the
// cursor is simply the last msisdn of the previous page.
type ExactRouteAdminStore interface {
	Get(ctx context.Context, msisdn string) (exact.Route, bool, error)
	List(ctx context.Context, after string, limit int) ([]exact.Route, error)
	Upsert(ctx context.Context, route exact.Route) (exact.Route, error)
	Delete(ctx context.Context, msisdn string) (bool, error)
	BulkUpsert(ctx context.Context, routes []exact.Route) error
}

// ImportRunner runs a bounded, fire-and-forget background job (the bulk MNP import). *async.Runner
// satisfies it. ErrBusy/ErrClosed surface as a retryable 503; the interface lives here, consumer-side.
type ImportRunner interface {
	Go(name string, job func(ctx context.Context) error) error
}

type exactRouteHandlers struct {
	store  ExactRouteAdminStore
	cache  ExactRouteCacheInvalidator
	reload ExactRouteReloadAnnouncer
	runner ImportRunner
	logger *slog.Logger
}

func registerExactRoutes(api huma.API, store ExactRouteAdminStore, cache ExactRouteCacheInvalidator,
	reload ExactRouteReloadAnnouncer, runner ImportRunner, logger *slog.Logger,
) {
	if logger == nil {
		logger = slog.Default()
	}
	if cache == nil {
		cache = noopRouteCache{}
	}
	if reload == nil {
		reload = noopRouteCache{}
	}
	h := &exactRouteHandlers{store: store, cache: cache, reload: reload, runner: runner, logger: logger}

	register(api, huma.Operation{
		OperationID: "import-exact-routes", Method: http.MethodPost, Path: "/admin/exact-routes/import",
		DefaultStatus: http.StatusAccepted,
		Summary:       "Bulk MNP import (async)", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.importRoutes)

	register(api, huma.Operation{
		OperationID: "list-exact-routes", Method: http.MethodGet, Path: "/admin/exact-routes",
		Summary: "List exact routes", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-exact-route", Method: http.MethodPost, Path: "/admin/exact-routes",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create/upsert an exact route (MSISDN -> connector|route)", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "lookup-exact-route", Method: http.MethodGet, Path: "/admin/exact-routes/lookup",
		Summary: "Look up the exact route for an MSISDN", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.lookup)

	register(api, huma.Operation{
		OperationID: "update-exact-route", Method: http.MethodPatch, Path: "/admin/exact-routes/{msisdn}",
		Summary: "Update an exact route", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-exact-route", Method: http.MethodDelete, Path: "/admin/exact-routes/{msisdn}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete an exact route", Tags: []string{"Exact Routes"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.delete)
}

type listExactRoutesInput struct {
	Cursor string `query:"cursor" doc:"Last msisdn of the previous page (the next_cursor returned by the prior page)."`
	Limit  int    `query:"limit" minimum:"1" maximum:"500" default:"50" doc:"Page size."`
}

type listExactRoutesOutput struct{ Body exactRoutePage }

func (h *exactRouteHandlers) list(ctx context.Context, in *listExactRoutesInput) (*listExactRoutesOutput, error) {
	// Fetch one extra row to decide has_more without a second query. The cursor is the last msisdn, so
	// the keyset resumes strictly after it (repo List uses `msisdn > after`).
	rows, err := h.store.List(ctx, in.Cursor, in.Limit+1)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listExactRoutesOutput{}
	hasMore := len(rows) > in.Limit
	if hasMore {
		rows = rows[:in.Limit]
	}
	out.Body.HasMore = hasMore
	if hasMore && len(rows) > 0 {
		out.Body.NextCursor = cursorString(rows[len(rows)-1].MSISDN)
	}
	out.Body.Data = make([]exactRouteDTO, 0, len(rows))
	for _, r := range rows {
		out.Body.Data = append(out.Body.Data, toExactRouteDTO(r))
	}
	return out, nil
}

// noopRouteCache stands in when no invalidator or announcer is wired (tests, and any deployment running
// without the data-plane cache). It is deliberately silent: the resolver still reads the durable table
// on a miss, and the next admin mutation still triggers a Bloom rebuild.
type noopRouteCache struct{}

func (noopRouteCache) Invalidate(context.Context, ...string) error { return nil }

func (noopRouteCache) Announce(context.Context) error { return nil }

// forget drops the cached routes of numbers whose durable row just changed. It is always called AFTER
// the commit: invalidating first lets a concurrent reader repopulate the pre-commit value and pin it for
// a whole TTL, since nothing else ever writes that key.
//
// A failure is logged, never returned. The row is already durable, so failing the request would send the
// operator to retry a write that succeeded; the resolver's TTL bounds how long a missed invalidation can
// matter, and both the upsert and the DEL are idempotent.
func (h *exactRouteHandlers) forget(ctx context.Context, msisdns ...string) {
	// Detached from cancellation, like the config-change publish next door: the row is already durable,
	// and a client that disconnects — or a runner draining — right after the write must not be what
	// leaves a stale route behind for a whole TTL. Bounded so a hung Redis cannot stall the caller.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), invalidateTimeout)
	defer cancel()

	if err := h.cache.Invalidate(fctx, msisdns...); err != nil {
		// Logged, not returned: see the note on forget. There is no counter here, matching
		// BalanceCacheInvalidator; the consequence — a route stale until the TTL, visible only in logs —
		// is recorded as an open follow-up on the step rather than silently accepted.
		h.logger.WarnContext(ctx, "exact-route cache invalidation failed",
			"numbers", len(msisdns), "err", err)
	}
}

// announceReload asks the fleet to rebuild its routing snapshot, for a mutation whose commit lands
// AFTER its HTTP response — the background bulk import. Every synchronous handler is already covered by
// the PublishConfigChanges middleware and must NOT call this.
func (h *exactRouteHandlers) announceReload(ctx context.Context) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), invalidateTimeout)
	defer cancel()

	if err := h.reload.Announce(actx); err != nil {
		h.logger.WarnContext(ctx, "exact-route reload announcement failed; "+
			"imported numbers stay out of the Bloom until the next admin mutation", "err", err)
	}
}

// invalidateTimeout bounds a post-commit cache operation, so a hung Redis cannot hold an admin request
// (or a draining import job) open. Same bound and same reasoning as the config-change publish.
const invalidateTimeout = 5 * time.Second

type createExactRouteInput struct{ Body exactRouteCreateBody }
type exactRouteOutput struct{ Body exactRouteDTO }

// create upserts a single exact route. It is a full replace keyed by msisdn: re-creating an existing
// number overwrites its source and clears imported_at (a manual override converts an imported row).
// Use PATCH (update) to change a target while preserving the import provenance.
func (h *exactRouteHandlers) create(ctx context.Context, in *createExactRouteInput) (*exactRouteOutput, error) {
	msisdn, err := normalizeMSISDN(in.Body.MSISDN)
	if err != nil {
		return nil, err
	}
	target, err := parseTarget(in.Body.TargetType, in.Body.TargetID)
	if err != nil {
		return nil, err
	}
	source := exact.SourceManual
	if in.Body.Source != "" {
		source = exact.Source(in.Body.Source)
	}
	saved, err := h.store.Upsert(ctx, exact.Route{MSISDN: msisdn, Target: target, Source: source})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	h.forget(ctx, msisdn)
	return &exactRouteOutput{Body: toExactRouteDTO(saved)}, nil
}

type importExactRoutesBody struct {
	Source string                 `json:"source,omitempty" enum:"mnp_import,carrier_feed"`
	Rows   []exactRouteCreateBody `json:"rows" maxItems:"10000"`
}

type importExactRoutesInput struct{ Body importExactRoutesBody }
type importExactRoutesOutput struct{ Body asyncJobDTO }

// importRoutes accepts a bulk MNP/carrier feed and returns 202 immediately. It validates and
// normalizes every row synchronously — a bad number is a 422 on this request, never a silently failed
// background job — then runs the idempotent BulkUpsert in the background so the HTTP connection is not
// held for the whole import. There is no status endpoint: the job_id is fire-and-forget, correlated
// with the completion/failure log line (spec §6.1; step-106 hot-reloads the Bloom after a large import).
func (h *exactRouteHandlers) importRoutes(_ context.Context, in *importExactRoutesInput) (*importExactRoutesOutput, error) {
	source := exact.SourceMNPImport
	if in.Body.Source != "" {
		source = exact.Source(in.Body.Source)
	}

	// Validate up front and dedupe by msisdn (last wins): a clean batch of plain data captured by the
	// background closure, with no request-scoped state.
	byMSISDN := make(map[string]int, len(in.Body.Rows))
	routes := make([]exact.Route, 0, len(in.Body.Rows))
	msisdns := make([]string, 0, len(in.Body.Rows))
	for _, row := range in.Body.Rows {
		msisdn, err := normalizeMSISDN(row.MSISDN)
		if err != nil {
			return nil, humaerr.FailValidation("invalid msisdn",
				humaerr.FieldError{Field: "rows", Message: "an entry is not a valid E.164 number"})
		}
		target, err := parseTarget(row.TargetType, row.TargetID)
		if err != nil {
			return nil, err
		}
		r := exact.Route{MSISDN: msisdn, Target: target, Source: source}
		if i, dup := byMSISDN[msisdn]; dup {
			routes[i] = r
			continue
		}
		byMSISDN[msisdn] = len(routes)
		routes = append(routes, r)
		msisdns = append(msisdns, msisdn)
	}

	jobID := uuid.NewString()
	logger := h.logger.With("job_id", jobID, "rows", len(routes), "source", string(source))
	err := h.runner.Go("import-exact-routes", func(jctx context.Context) error {
		berr := h.store.BulkUpsert(jctx, routes)
		// Invalidate and announce even on failure: BulkUpsert is a pgx.Batch, so a mid-batch error can
		// still have committed rows. Skipping here would leave exactly those rows stale for a TTL, and
		// out of the Bloom until some unrelated admin mutation happens by.
		h.forget(jctx, msisdns...)
		h.announceReload(jctx)
		if berr != nil {
			return berr
		}
		logger.Info("exact-route import completed")
		return nil
	})
	if err != nil {
		// The runner is saturated or shutting down: a retryable 503, undeclared (a near-unreachable
		// operational state on an admin-only endpoint), not a client error.
		if errors.Is(err, async.ErrBusy) || errors.Is(err, async.ErrClosed) {
			return nil, humaerr.FromError(errs.ErrServiceUnavailable)
		}
		return nil, humaerr.FromError(err)
	}

	now := time.Now().UTC()
	return &importExactRoutesOutput{Body: asyncJobDTO{
		JobID: jobID, Status: "queued", Progress: ptr(0.0), CreatedAt: now,
	}}, nil
}

type msisdnPathInput struct {
	MSISDN string `path:"msisdn"`
}

type updateExactRouteInput struct {
	MSISDN string `path:"msisdn"`
	Body   exactRouteUpdateBody
}

func (h *exactRouteHandlers) update(ctx context.Context, in *updateExactRouteInput) (*exactRouteOutput, error) {
	msisdn, err := normalizeMSISDN(in.MSISDN)
	if err != nil {
		return nil, err
	}
	current, found, err := h.store.Get(ctx, msisdn)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("exact route")
	}

	// target_type and target_id are a unit (a connector id is meaningless as a route id): change both
	// together or neither, so a PATCH cannot leave the row pointing a target id at the wrong kind.
	if (in.Body.TargetType == nil) != (in.Body.TargetID == nil) {
		return nil, humaerr.FailValidation("incomplete target",
			humaerr.FieldError{Field: "target_id", Message: "target_type and target_id must be set together"})
	}
	if in.Body.TargetType != nil {
		target, terr := parseTarget(*in.Body.TargetType, *in.Body.TargetID)
		if terr != nil {
			return nil, terr
		}
		current.Target = target
	}
	if in.Body.Source != nil {
		current.Source = exact.Source(*in.Body.Source)
	}

	saved, err := h.store.Upsert(ctx, current) // preserves ImportedAt, refreshes updated_at
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	h.forget(ctx, msisdn)
	return &exactRouteOutput{Body: toExactRouteDTO(saved)}, nil
}

func (h *exactRouteHandlers) delete(ctx context.Context, in *msisdnPathInput) (*deleteOutput, error) {
	msisdn, err := normalizeMSISDN(in.MSISDN)
	if err != nil {
		return nil, notFound("exact route")
	}
	found, err := h.store.Delete(ctx, msisdn)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		// No row, but possibly a stale key: a lost invalidation, or the cache-aside window on an earlier
		// delete, can leave one behind. DELETE is then the operator's ONLY lever to purge it, and a 404
		// that clears nothing disarms it precisely when it is needed. The DEL is idempotent.
		h.forget(ctx, msisdn)
		return nil, notFound("exact route")
	}
	h.forget(ctx, msisdn)
	return &deleteOutput{}, nil
}

type lookupExactRouteInput struct {
	MSISDN string `query:"msisdn" required:"true" doc:"MSISDN to resolve (E.164)."`
}

func (h *exactRouteHandlers) lookup(ctx context.Context, in *lookupExactRouteInput) (*exactRouteOutput, error) {
	msisdn, err := normalizeMSISDN(in.MSISDN)
	if err != nil {
		return nil, err
	}
	r, found, err := h.store.Get(ctx, msisdn)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	if !found {
		return nil, notFound("exact route")
	}
	return &exactRouteOutput{Body: toExactRouteDTO(r)}, nil
}

// parseTarget validates a target_type/target_id pair into an exact.Target. The enum tag already bounds
// target_type at the schema; this guards the uuid and keeps the domain type the single constructor.
func parseTarget(targetType, targetID string) (exact.Target, error) {
	id, err := uuid.Parse(targetID)
	if err != nil {
		return exact.Target{}, humaerr.FailValidation("invalid target",
			humaerr.FieldError{Field: "target_id", Message: "must be a UUID"})
	}
	return exact.Target{Type: exact.TargetType(targetType), ID: id}, nil
}
