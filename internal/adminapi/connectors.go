package adminapi

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/auth"
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
	ReconnectMultiplier         *float64       `json:"reconnect_multiplier,omitempty" minimum:"1"`
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
	Password              string         `json:"password" doc:"Write-only; stored hashed, never returned."`
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
	Password              *string        `json:"password,omitempty"`
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
	store ConnectorStore
}

func registerConnectors(api huma.API, store ConnectorStore) {
	h := &connectorHandlers{store: store}

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
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound},
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
		Errors:   []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict},
	}, h.delete)
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
