package adminapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
	"github.com/martialanouman/go-gateway/internal/connector/status"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	humaerr "github.com/martialanouman/go-gateway/internal/platform/errors/humaerr"
)

// hashConnectorPassword hashes the write-only SMSC bind password with argon2id (the outbound bind is
// authenticated rarely, so a slow hash is correct — the same scheme as an inbound bind password).
func hashConnectorPassword(password string) (string, error) {
	hash, err := credential.HashBindPassword(password)
	if err != nil {
		return "", humaerr.Fail(errs.ErrInternal, "hash connector password")
	}
	return hash, nil
}

// connectorDTO is the wire form of a Connector (contract schema Connector). Only ten fields are
// required by the contract; the rest are optional-and-non-null, rendered as pointers that the
// handler always sets, so the value still always appears. The password is write-only and has no
// field here. jsonb, smallint and numeric columns are already int / float64 / map by the time they
// reach this layer (converted in the repository).
type connectorDTO struct {
	ID                          string         `json:"id" format:"uuid"`
	Name                        string         `json:"name"`
	Host                        string         `json:"host"`
	Port                        int            `json:"port" minimum:"1" maximum:"65535"`
	BindType                    string         `json:"bind_type" enum:"tx,rx,trx"`
	SystemID                    string         `json:"system_id"`
	VendorProfile               *string        `json:"vendor_profile,omitempty" nullable:"true"`
	SystemType                  *string        `json:"system_type,omitempty"`
	InterfaceVersion            *int           `json:"interface_version,omitempty"`
	AddrTON                     *int           `json:"addr_ton,omitempty"`
	AddrNPI                     *int           `json:"addr_npi,omitempty"`
	AddressRange                *string        `json:"address_range,omitempty"`
	SourceAddrTON               *int           `json:"source_addr_ton,omitempty"`
	SourceAddrNPI               *int           `json:"source_addr_npi,omitempty"`
	DestAddrTON                 *int           `json:"dest_addr_ton,omitempty"`
	DestAddrNPI                 *int           `json:"dest_addr_npi,omitempty"`
	DataCodingDefault           *int           `json:"data_coding_default,omitempty" nullable:"true"`
	RegisteredDeliveryDefault   *int           `json:"registered_delivery_default,omitempty"`
	ReplaceIfPresentFlagDefault *int           `json:"replace_if_present_flag_default,omitempty"`
	EsmClassDefault             *int           `json:"esm_class_default,omitempty"`
	PriorityFlagDefault         *int           `json:"priority_flag_default,omitempty"`
	ValidityPeriodDefault       *string        `json:"validity_period_default,omitempty" nullable:"true"`
	SmDefaultMsgID              *int           `json:"sm_default_msg_id,omitempty"`
	EnquireLinkIntervalSec      *int           `json:"enquire_link_interval_sec,omitempty" minimum:"1"`
	EnquireLinkMaxMissed        *int           `json:"enquire_link_max_missed,omitempty" minimum:"1"`
	BindTimeoutMs               *int           `json:"bind_timeout_ms,omitempty" minimum:"1"`
	ResponseTimeoutMs           *int           `json:"response_timeout_ms,omitempty" minimum:"1"`
	WindowSize                  int            `json:"window_size" minimum:"1"`
	BindPoolSize                int            `json:"bind_pool_size" minimum:"1" maximum:"32"`
	ThroughputLimitPerSec       *int           `json:"throughput_limit_per_sec,omitempty" minimum:"1" nullable:"true"`
	TLSEnabled                  *bool          `json:"tls_enabled,omitempty"`
	TLSConfigJSON               map[string]any `json:"tls_config_json,omitempty" nullable:"true"`
	PriorityTier                *int           `json:"priority_tier,omitempty"`
	Status                      string         `json:"status" enum:"active,degraded,disabled"`
	AutoReconnectEnabled        bool           `json:"auto_reconnect_enabled"`
	ReconnectInitialDelayMs     *int           `json:"reconnect_initial_delay_ms,omitempty" minimum:"1"`
	ReconnectMultiplier         *float64       `json:"reconnect_multiplier,omitempty" minimum:"1" maximum:"99.99"`
	ReconnectMaxDelayMs         *int           `json:"reconnect_max_delay_ms,omitempty" minimum:"1"`
	ReconnectJitterPct          *int           `json:"reconnect_jitter_pct,omitempty" minimum:"0" maximum:"100"`
	ReconnectMaxAttempts        *int           `json:"reconnect_max_attempts,omitempty" minimum:"0"`
	CreatedAt                   *time.Time     `json:"created_at,omitempty" format:"date-time"`
	UpdatedAt                   *time.Time     `json:"updated_at,omitempty" format:"date-time"`
}

func toConnectorDTO(c cp.Connector) connectorDTO {
	return connectorDTO{
		ID:                          idString(c.ID),
		Name:                        c.Name,
		Host:                        c.Host,
		Port:                        c.Port,
		BindType:                    string(c.BindType),
		SystemID:                    c.SystemID,
		VendorProfile:               c.VendorProfile,
		SystemType:                  ptr(c.SystemType),
		InterfaceVersion:            ptr(c.InterfaceVersion),
		AddrTON:                     ptr(c.AddrTON),
		AddrNPI:                     ptr(c.AddrNPI),
		AddressRange:                ptr(c.AddressRange),
		SourceAddrTON:               ptr(c.SourceAddrTON),
		SourceAddrNPI:               ptr(c.SourceAddrNPI),
		DestAddrTON:                 ptr(c.DestAddrTON),
		DestAddrNPI:                 ptr(c.DestAddrNPI),
		DataCodingDefault:           c.DataCodingDefault,
		RegisteredDeliveryDefault:   ptr(c.RegisteredDeliveryDefault),
		ReplaceIfPresentFlagDefault: ptr(c.ReplaceIfPresentFlagDefault),
		EsmClassDefault:             ptr(c.EsmClassDefault),
		PriorityFlagDefault:         ptr(c.PriorityFlagDefault),
		ValidityPeriodDefault:       c.ValidityPeriodDefault,
		SmDefaultMsgID:              ptr(c.SmDefaultMsgID),
		EnquireLinkIntervalSec:      ptr(c.EnquireLinkIntervalSec),
		EnquireLinkMaxMissed:        ptr(c.EnquireLinkMaxMissed),
		BindTimeoutMs:               ptr(c.BindTimeoutMs),
		ResponseTimeoutMs:           ptr(c.ResponseTimeoutMs),
		WindowSize:                  c.WindowSize,
		BindPoolSize:                c.BindPoolSize,
		ThroughputLimitPerSec:       c.ThroughputLimitPerSec,
		TLSEnabled:                  ptr(c.TLSEnabled),
		TLSConfigJSON:               c.TLSConfigJSON,
		PriorityTier:                ptr(c.PriorityTier),
		Status:                      string(c.Status),
		AutoReconnectEnabled:        c.AutoReconnectEnabled,
		ReconnectInitialDelayMs:     ptr(c.ReconnectInitialDelayMs),
		ReconnectMultiplier:         ptr(c.ReconnectMultiplier),
		ReconnectMaxDelayMs:         ptr(c.ReconnectMaxDelayMs),
		ReconnectJitterPct:          ptr(c.ReconnectJitterPct),
		ReconnectMaxAttempts:        ptr(c.ReconnectMaxAttempts),
		CreatedAt:                   ptr(c.CreatedAt),
		UpdatedAt:                   ptr(c.UpdatedAt),
	}
}

type connectorCreateBody struct {
	Name                  string         `json:"name"`
	Host                  string         `json:"host"`
	Port                  int            `json:"port" minimum:"1" maximum:"65535"`
	BindType              string         `json:"bind_type" enum:"tx,rx,trx"`
	SystemID              string         `json:"system_id"`
	Password              string         `json:"password" minLength:"1" doc:"Write-only; stored hashed, never returned."`
	VendorProfile         *string        `json:"vendor_profile,omitempty" nullable:"true"`
	InterfaceVersion      *int           `json:"interface_version,omitempty"`
	DataCodingDefault     *int           `json:"data_coding_default,omitempty" nullable:"true"`
	WindowSize            *int           `json:"window_size,omitempty" minimum:"1"`
	BindPoolSize          *int           `json:"bind_pool_size,omitempty" minimum:"1" maximum:"32"`
	ThroughputLimitPerSec *int           `json:"throughput_limit_per_sec,omitempty" minimum:"1" nullable:"true"`
	TLSEnabled            *bool          `json:"tls_enabled,omitempty"`
	TLSConfigJSON         map[string]any `json:"tls_config_json,omitempty" nullable:"true"`
	PriorityTier          *int           `json:"priority_tier,omitempty"`
	AutoReconnectEnabled  *bool          `json:"auto_reconnect_enabled,omitempty"`
}

type connectorUpdateBody struct {
	Name                  *string        `json:"name,omitempty"`
	Host                  *string        `json:"host,omitempty"`
	Port                  *int           `json:"port,omitempty" minimum:"1" maximum:"65535"`
	BindType              *string        `json:"bind_type,omitempty" enum:"tx,rx,trx"`
	SystemID              *string        `json:"system_id,omitempty"`
	Password              *string        `json:"password,omitempty" minLength:"1"`
	VendorProfile         *string        `json:"vendor_profile,omitempty" nullable:"true"`
	DataCodingDefault     *int           `json:"data_coding_default,omitempty" nullable:"true"`
	WindowSize            *int           `json:"window_size,omitempty" minimum:"1"`
	ThroughputLimitPerSec *int           `json:"throughput_limit_per_sec,omitempty" minimum:"1" nullable:"true"`
	TLSEnabled            *bool          `json:"tls_enabled,omitempty"`
	TLSConfigJSON         map[string]any `json:"tls_config_json,omitempty" nullable:"true"`
	PriorityTier          *int           `json:"priority_tier,omitempty"`
	Status                *string        `json:"status,omitempty" enum:"active,degraded,disabled"`
}

type connectorHandlers struct {
	store   ConnectorStore
	control ConnectorControl
}

func registerConnectors(api huma.API, store ConnectorStore, control ConnectorControl) {
	h := &connectorHandlers{store: store, control: control}

	register(api, huma.Operation{
		OperationID: "list-connectors", Method: http.MethodGet, Path: "/admin/connectors",
		Summary: "List connectors", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden},
	}, h.list)

	register(api, huma.Operation{
		OperationID: "create-connector", Method: http.MethodPost, Path: "/admin/connectors",
		DefaultStatus: http.StatusCreated,
		Summary:       "Create a connector", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusUnprocessableEntity, http.StatusConflict},
	}, h.create)

	register(api, huma.Operation{
		OperationID: "get-connector", Method: http.MethodGet, Path: "/admin/connectors/{id}",
		Summary: "Get a connector", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.get)

	register(api, huma.Operation{
		OperationID: "update-connector", Method: http.MethodPatch, Path: "/admin/connectors/{id}",
		Summary: "Update a connector", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.update)

	register(api, huma.Operation{
		OperationID: "delete-connector", Method: http.MethodDelete, Path: "/admin/connectors/{id}",
		DefaultStatus: http.StatusNoContent,
		Summary:       "Delete a connector", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity},
	}, h.delete)

	// Connector piloting (step-128): rebind, live status, reconnect policy, bind-pool resize.
	register(api, huma.Operation{
		OperationID: "rebind-connector", Method: http.MethodPost, Path: "/admin/connectors/{id}/rebind",
		DefaultStatus: http.StatusAccepted,
		Summary:       "Manually rebind (reconnect) a connector", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.rebind)

	register(api, huma.Operation{
		OperationID: "get-connector-status", Method: http.MethodGet, Path: "/admin/connectors/{id}/status",
		Summary: "Live link_status + breaker_state, per bind in the pool", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminRead),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
	}, h.status)

	register(api, huma.Operation{
		OperationID: "set-connector-reconnect-policy", Method: http.MethodPatch, Path: "/admin/connectors/{id}/reconnect-policy",
		Summary: "Set auto-reconnect policy", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.setReconnectPolicy)

	register(api, huma.Operation{
		OperationID: "set-connector-bind-pool", Method: http.MethodPatch, Path: "/admin/connectors/{id}/bind-pool",
		Summary: "Resize the bind pool", Tags: []string{"Connectors"},
		Security: scopeSecurity(auth.ScopeAdminWrite),
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusUnprocessableEntity},
	}, h.setBindPool)
}

type listConnectorsOutput struct {
	Body []connectorDTO
}

func (h *connectorHandlers) list(ctx context.Context, _ *struct{}) (*listConnectorsOutput, error) {
	conns, err := h.store.List(ctx)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	out := &listConnectorsOutput{Body: make([]connectorDTO, 0, len(conns))}
	for _, c := range conns {
		out.Body = append(out.Body, toConnectorDTO(c))
	}
	return out, nil
}

type createConnectorInput struct{ Body connectorCreateBody }
type connectorOutput struct{ Body connectorDTO }

func (h *connectorHandlers) create(ctx context.Context, in *createConnectorInput) (*connectorOutput, error) {
	hash, err := hashConnectorPassword(in.Body.Password)
	if err != nil {
		return nil, err
	}
	c, err := h.store.Create(ctx, cp.NewConnector{
		Name:                  in.Body.Name,
		Host:                  in.Body.Host,
		Port:                  in.Body.Port,
		BindType:              cp.BindType(in.Body.BindType),
		SystemID:              in.Body.SystemID,
		PasswordHash:          hash,
		VendorProfile:         in.Body.VendorProfile,
		InterfaceVersion:      in.Body.InterfaceVersion,
		DataCodingDefault:     in.Body.DataCodingDefault,
		WindowSize:            in.Body.WindowSize,
		BindPoolSize:          in.Body.BindPoolSize,
		ThroughputLimitPerSec: in.Body.ThroughputLimitPerSec,
		TLSEnabled:            in.Body.TLSEnabled,
		TLSConfigJSON:         in.Body.TLSConfigJSON,
		PriorityTier:          in.Body.PriorityTier,
		AutoReconnectEnabled:  in.Body.AutoReconnectEnabled,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &connectorOutput{Body: toConnectorDTO(c)}, nil
}

type connectorIDInput struct {
	ID string `path:"id" format:"uuid"`
}

func (h *connectorHandlers) get(ctx context.Context, in *connectorIDInput) (*connectorOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("connector")
	}
	c, err := h.store.Get(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &connectorOutput{Body: toConnectorDTO(c)}, nil
}

type updateConnectorInput struct {
	ID   string `path:"id" format:"uuid"`
	Body connectorUpdateBody
}

func (h *connectorHandlers) update(ctx context.Context, in *updateConnectorInput) (*connectorOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("connector")
	}
	// A connector's throughput_limit_per_sec is its hard technical ceiling; its operational rate_limit
	// must never exceed it (spec §6.4 NOTE — an application check, there being no cross-table CHECK).
	// Reject an update that would lower the ceiling below the configured operational limit.
	if err := h.checkThroughputAtLeastRateLimit(ctx, id, in.Body.ThroughputLimitPerSec); err != nil {
		return nil, err
	}
	patch := cp.ConnectorPatch{
		Name:                  in.Body.Name,
		Host:                  in.Body.Host,
		Port:                  in.Body.Port,
		BindType:              enumPtr[cp.BindType](in.Body.BindType),
		SystemID:              in.Body.SystemID,
		VendorProfile:         in.Body.VendorProfile,
		DataCodingDefault:     in.Body.DataCodingDefault,
		WindowSize:            in.Body.WindowSize,
		ThroughputLimitPerSec: in.Body.ThroughputLimitPerSec,
		TLSEnabled:            in.Body.TLSEnabled,
		TLSConfigJSON:         in.Body.TLSConfigJSON,
		PriorityTier:          in.Body.PriorityTier,
		Status:                enumPtr[cp.ConnectorStatus](in.Body.Status),
	}
	if in.Body.Password != nil {
		hash, err := hashConnectorPassword(*in.Body.Password)
		if err != nil {
			return nil, err
		}
		patch.PasswordHash = &hash
	}
	c, err := h.store.Update(ctx, id, patch)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &connectorOutput{Body: toConnectorDTO(c)}, nil
}

// checkThroughputAtLeastRateLimit rejects (422) an update that would set a connector's technical ceiling
// below its configured operational rate_limit. A nil throughput (unchanged) or a connector with no
// operational limit passes.
func (h *connectorHandlers) checkThroughputAtLeastRateLimit(ctx context.Context, connectorID uuid.UUID, throughput *int) error {
	if throughput == nil {
		return nil
	}
	limit, ok, err := h.store.RateLimit(ctx, connectorID)
	if err != nil {
		return humaerr.FromError(err)
	}
	if !ok || limit.MaxPerSec == nil || *throughput >= *limit.MaxPerSec {
		return nil
	}
	return humaerr.FailValidation("throughput_limit_per_sec below the connector's operational rate limit",
		humaerr.FieldError{
			Field:   "throughput_limit_per_sec",
			Message: fmt.Sprintf("must be >= the connector's rate_limit max_per_sec (%d)", *limit.MaxPerSec),
		})
}

func (h *connectorHandlers) delete(ctx context.Context, in *connectorIDInput) (*deleteOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("connector")
	}
	if err := h.store.Delete(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	return &deleteOutput{}, nil
}

// --- Connector piloting (step-128) ---

type bindStatusDTO struct {
	BindIndex    int    `json:"bind_index"`
	PodID        string `json:"pod_id,omitempty"`
	LinkStatus   string `json:"link_status" enum:"up,reconnecting,down"`
	BreakerState string `json:"breaker_state" enum:"closed,open,half_open"`
	InFlight     int    `json:"in_flight,omitempty"`
}

type connectorStatusDTO struct {
	ConnectorID  string          `json:"connector_id" format:"uuid"`
	BreakerState string          `json:"breaker_state" enum:"closed,open,half_open"`
	Binds        []bindStatusDTO `json:"binds"`
}

type connectorStatusOutput struct{ Body connectorStatusDTO }

func toStatusDTO(c status.Connector) connectorStatusDTO {
	binds := make([]bindStatusDTO, 0, len(c.Binds))
	for _, b := range c.Binds {
		binds = append(binds, bindStatusDTO{
			BindIndex: b.BindIndex, PodID: b.PodID, LinkStatus: b.LinkStatus,
			BreakerState: b.BreakerState, InFlight: b.InFlight,
		})
	}
	return connectorStatusDTO{ConnectorID: c.ConnectorID.String(), BreakerState: c.BreakerState, Binds: binds}
}

// rebind signals the connector's pods to drop and re-establish their binds. The connector must exist;
// the response is the (immediately-read) live status, which will show the link transitioning.
func (h *connectorHandlers) rebind(ctx context.Context, in *connectorIDInput) (*connectorStatusOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("connector")
	}
	if _, err := h.store.Get(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	if err := h.control.SignalReconfigure(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	// The signal IS the rebind, and it succeeded — so never fail the 202 on the follow-up status read.
	// A read error just yields the empty (last-known) status; the client re-polls get-connector-status.
	st, err := h.control.Read(ctx, id)
	if err != nil {
		st = status.Connector{ConnectorID: id, BreakerState: "closed"}
	}
	return &connectorStatusOutput{Body: toStatusDTO(st)}, nil
}

// status returns the connector's live per-bind link_status + breaker_state (kept distinct). An unknown
// connector is 404; a live connector with nothing published yet returns an empty bind list.
func (h *connectorHandlers) status(ctx context.Context, in *connectorIDInput) (*connectorStatusOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("connector")
	}
	if _, err := h.store.Get(ctx, id); err != nil {
		return nil, humaerr.FromError(err)
	}
	st, err := h.control.Read(ctx, id)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	return &connectorStatusOutput{Body: toStatusDTO(st)}, nil
}

type reconnectPolicyBody struct {
	AutoReconnectEnabled bool     `json:"auto_reconnect_enabled"`
	InitialDelayMs       *int     `json:"reconnect_initial_delay_ms,omitempty" minimum:"1"`
	Multiplier           *float64 `json:"reconnect_multiplier,omitempty" minimum:"1" maximum:"99.99"`
	MaxDelayMs           *int     `json:"reconnect_max_delay_ms,omitempty" minimum:"1"`
	JitterPct            *int     `json:"reconnect_jitter_pct,omitempty" minimum:"0" maximum:"100"`
	MaxAttempts          *int     `json:"reconnect_max_attempts,omitempty" minimum:"0"`
}

type reconnectPolicyInput struct {
	ID   string `path:"id" format:"uuid"`
	Body reconnectPolicyBody
}

// setReconnectPolicy persists the auto-reconnection policy, then signals the pods to pick it up. The
// persist is authoritative; a failed signal is not fatal (the pool re-reads on its next re-dial).
func (h *connectorHandlers) setReconnectPolicy(ctx context.Context, in *reconnectPolicyInput) (*connectorOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("connector")
	}
	c, err := h.store.UpdateReconnectPolicy(ctx, id, cp.ReconnectPolicy{
		AutoReconnectEnabled: in.Body.AutoReconnectEnabled,
		InitialDelayMs:       in.Body.InitialDelayMs,
		Multiplier:           in.Body.Multiplier,
		MaxDelayMs:           in.Body.MaxDelayMs,
		JitterPct:            in.Body.JitterPct,
		MaxAttempts:          in.Body.MaxAttempts,
	})
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	_ = h.control.SignalReconfigure(ctx, id) //nolint:errcheck // best-effort: the persist is authoritative
	return &connectorOutput{Body: toConnectorDTO(c)}, nil
}

type bindPoolBody struct {
	BindPoolSize int `json:"bind_pool_size" minimum:"1" maximum:"32" doc:"Parallel binds PER POOL POD; with R replicas the SMSC sees R x bind_pool_size binds."`
}

type bindPoolInput struct {
	ID   string `path:"id" format:"uuid"`
	Body bindPoolBody
}

// setBindPool persists the new bind_pool_size, then signals the pods to re-dial at the new size. The
// persist is authoritative; a failed signal is not fatal (the pool re-reads on its next re-dial).
func (h *connectorHandlers) setBindPool(ctx context.Context, in *bindPoolInput) (*connectorOutput, error) {
	id, err := uuid.Parse(in.ID)
	if err != nil {
		return nil, notFound("connector")
	}
	c, err := h.store.UpdateBindPool(ctx, id, in.Body.BindPoolSize)
	if err != nil {
		return nil, humaerr.FromError(err)
	}
	_ = h.control.SignalReconfigure(ctx, id) //nolint:errcheck // best-effort: the persist is authoritative
	return &connectorOutput{Body: toConnectorDTO(c)}, nil
}
