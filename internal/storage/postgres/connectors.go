package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/storage/postgres/sqlcgen"
)

// ConnectorRepo is the SMSC connectors repository. It satisfies adminapi.ConnectorStore
// structurally. This is where the smallint / numeric / jsonb columns are converted to the int /
// float64 / map[string]any the domain and the contract use.
type ConnectorRepo struct {
	q *sqlcgen.Queries
}

// NewConnectorRepo returns the connectors repository backed by pool.
func NewConnectorRepo(pool *pgxpool.Pool) *ConnectorRepo {
	return &ConnectorRepo{q: sqlcgen.New(pool)}
}

// Create inserts a connector. A duplicate name violates the inline UNIQUE constraint, which
// translate() reports as a conflict (409).
func (r *ConnectorRepo) Create(ctx context.Context, in cp.NewConnector) (cp.Connector, error) {
	tls, err := jsonbBytes(in.TLSConfigJSON)
	if err != nil {
		return cp.Connector{}, fmt.Errorf("create connector: encode tls config: %w", errs.ErrValidation)
	}
	row, err := r.q.CreateConnector(ctx, sqlcgen.CreateConnectorParams{
		Name:                  in.Name,
		Host:                  in.Host,
		Port:                  int32(in.Port), //nolint:gosec // G115: port is validated 1..65535 by the API.
		BindType:              string(in.BindType),
		SystemID:              in.SystemID,
		PasswordHash:          in.PasswordHash,
		VendorProfile:         in.VendorProfile,
		InterfaceVersion:      i16ptr(in.InterfaceVersion),
		DataCodingDefault:     i16ptr(in.DataCodingDefault),
		WindowSize:            i32ptr(in.WindowSize),
		BindPoolSize:          i32ptr(in.BindPoolSize),
		ThroughputLimitPerSec: i32ptr(in.ThroughputLimitPerSec),
		TlsEnabled:            in.TLSEnabled,
		TlsConfigJson:         tls,
		PriorityTier:          i32ptr(in.PriorityTier),
		AutoReconnectEnabled:  in.AutoReconnectEnabled,
	})
	if err != nil {
		return cp.Connector{}, translate("create connector", err)
	}
	return connectorFromRow(row)
}

// Get returns the connector with id, or ErrNotFound.
func (r *ConnectorRepo) Get(ctx context.Context, id uuid.UUID) (cp.Connector, error) {
	row, err := r.q.GetConnector(ctx, id)
	if err != nil {
		return cp.Connector{}, translate("get connector", err)
	}
	return connectorFromRow(row)
}

// List returns every connector, ordered by name. The contract returns a bare array (no pagination).
func (r *ConnectorRepo) List(ctx context.Context) ([]cp.Connector, error) {
	rows, err := r.q.ListConnectors(ctx)
	if err != nil {
		return nil, translate("list connectors", err)
	}
	out := make([]cp.Connector, 0, len(rows))
	for _, row := range rows {
		c, err := connectorFromRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// Update applies a partial change and returns the connector, or ErrNotFound.
func (r *ConnectorRepo) Update(ctx context.Context, id uuid.UUID, p cp.ConnectorPatch) (cp.Connector, error) {
	tls, err := jsonbBytes(p.TLSConfigJSON)
	if err != nil {
		return cp.Connector{}, fmt.Errorf("update connector: encode tls config: %w", errs.ErrValidation)
	}
	row, err := r.q.UpdateConnector(ctx, sqlcgen.UpdateConnectorParams{
		ID:                    id,
		Name:                  p.Name,
		Host:                  p.Host,
		Port:                  i32ptr(p.Port),
		BindType:              strPtr(p.BindType),
		SystemID:              p.SystemID,
		PasswordHash:          p.PasswordHash,
		VendorProfile:         p.VendorProfile,
		DataCodingDefault:     i16ptr(p.DataCodingDefault),
		WindowSize:            i32ptr(p.WindowSize),
		ThroughputLimitPerSec: i32ptr(p.ThroughputLimitPerSec),
		TlsEnabled:            p.TLSEnabled,
		TlsConfigJson:         tls,
		PriorityTier:          i32ptr(p.PriorityTier),
		Status:                strPtr(p.Status),
	})
	if err != nil {
		return cp.Connector{}, translate("update connector", err)
	}
	return connectorFromRow(row)
}

// Delete removes a connector, or reports ErrNotFound when nothing matched.
func (r *ConnectorRepo) Delete(ctx context.Context, id uuid.UUID) error {
	n, err := r.q.DeleteConnector(ctx, id)
	if err != nil {
		return translate("delete connector", err)
	}
	if n == 0 {
		return errs.ErrNotFound
	}
	return nil
}

func connectorFromRow(row sqlcgen.ControlPlaneSmscConnector) (cp.Connector, error) {
	tls, err := jsonbMap(row.TlsConfigJson)
	if err != nil {
		// A jsonb column this service wrote should always decode; if it does not, that is an
		// internal fault, not client input.
		return cp.Connector{}, fmt.Errorf("decode connector tls config: %w", errs.ErrInternal)
	}
	return cp.Connector{
		ID:                          row.ID,
		Name:                        row.Name,
		Host:                        row.Host,
		Port:                        int(row.Port),
		BindType:                    cp.BindType(row.BindType),
		SystemID:                    row.SystemID,
		VendorProfile:               row.VendorProfile,
		SystemType:                  row.SystemType,
		InterfaceVersion:            int(row.InterfaceVersion),
		AddrTON:                     int(row.AddrTon),
		AddrNPI:                     int(row.AddrNpi),
		AddressRange:                row.AddressRange,
		SourceAddrTON:               int(row.SourceAddrTon),
		SourceAddrNPI:               int(row.SourceAddrNpi),
		DestAddrTON:                 int(row.DestAddrTon),
		DestAddrNPI:                 int(row.DestAddrNpi),
		DataCodingDefault:           int16ptr(row.DataCodingDefault),
		RegisteredDeliveryDefault:   int(row.RegisteredDeliveryDefault),
		ReplaceIfPresentFlagDefault: int(row.ReplaceIfPresentFlagDefault),
		EsmClassDefault:             int(row.EsmClassDefault),
		PriorityFlagDefault:         int(row.PriorityFlagDefault),
		ValidityPeriodDefault:       row.ValidityPeriodDefault,
		SmDefaultMsgID:              int(row.SmDefaultMsgID),
		EnquireLinkIntervalSec:      int(row.EnquireLinkIntervalSec),
		EnquireLinkMaxMissed:        int(row.EnquireLinkMaxMissed),
		BindTimeoutMs:               int(row.BindTimeoutMs),
		ResponseTimeoutMs:           int(row.ResponseTimeoutMs),
		WindowSize:                  int(row.WindowSize),
		BindPoolSize:                int(row.BindPoolSize),
		ThroughputLimitPerSec:       intptr(row.ThroughputLimitPerSec),
		TLSEnabled:                  row.TlsEnabled,
		TLSConfigJSON:               tls,
		PriorityTier:                int(row.PriorityTier),
		Status:                      cp.ConnectorStatus(row.Status),
		AutoReconnectEnabled:        row.AutoReconnectEnabled,
		ReconnectInitialDelayMs:     int(row.ReconnectInitialDelayMs),
		ReconnectMultiplier:         numFloat(row.ReconnectMultiplier),
		ReconnectMaxDelayMs:         int(row.ReconnectMaxDelayMs),
		ReconnectJitterPct:          int(row.ReconnectJitterPct),
		ReconnectMaxAttempts:        int(row.ReconnectMaxAttempts),
		CreatedAt:                   tsVal(row.CreatedAt),
		UpdatedAt:                   tsVal(row.UpdatedAt),
	}, nil
}
