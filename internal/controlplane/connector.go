package controlplane

import (
	"time"

	"github.com/google/uuid"
)

// Connector is an outbound SMSC link (control_plane.smsc_connectors). The domain type uses plain
// Go types — int, float64, map[string]any — where the DDL uses smallint, numeric and jsonb; the
// storage layer converts across that gap so nothing above it ever sees a pgtype. Password is the bind
// password SEALED (ADR-0016): it cannot be hashed, because an outbound bind puts it in clear in the
// bind_transceiver PDU. Carrying it here is safe — sealed bytes are not the secret — and the Admin API
// still never returns it.
type Connector struct {
	ID            uuid.UUID
	Name          string
	Host          string
	Port          int
	BindType      BindType
	SystemID      string
	Password      SealedSecret
	VendorProfile *string

	SystemType                  string
	InterfaceVersion            int
	AddrTON                     int
	AddrNPI                     int
	AddressRange                string
	SourceAddrTON               int
	SourceAddrNPI               int
	DestAddrTON                 int
	DestAddrNPI                 int
	DataCodingDefault           *int
	RegisteredDeliveryDefault   int
	ReplaceIfPresentFlagDefault int
	EsmClassDefault             int
	PriorityFlagDefault         int
	ValidityPeriodDefault       *string
	SmDefaultMsgID              int

	EnquireLinkIntervalSec int
	EnquireLinkMaxMissed   int
	BindTimeoutMs          int
	ResponseTimeoutMs      int
	WindowSize             int
	BindPoolSize           int
	ThroughputLimitPerSec  *int

	TLSEnabled    bool
	TLSConfigJSON map[string]any
	PriorityTier  int

	Status               ConnectorStatus
	AutoReconnectEnabled bool

	ReconnectInitialDelayMs int
	ReconnectMultiplier     float64
	ReconnectMaxDelayMs     int
	ReconnectJitterPct      int
	ReconnectMaxAttempts    int

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewConnector is the input to create a connector. It exposes only the fields the contract's
// ConnectorCreate settles at creation; the SMPP wire-parameter block and the reconnect tuning
// knobs take their DDL defaults (or a vendor profile) and are adjusted later through their own
// endpoints. Password is the write-only bind password, already sealed by ConfigSecrets.
type NewConnector struct {
	Name                  string
	Host                  string
	Port                  int
	BindType              BindType
	SystemID              string
	Password              SealedSecret
	VendorProfile         *string
	InterfaceVersion      *int
	DataCodingDefault     *int
	WindowSize            *int
	BindPoolSize          *int
	ThroughputLimitPerSec *int
	TLSEnabled            *bool
	TLSConfigJSON         map[string]any
	PriorityTier          *int
	AutoReconnectEnabled  *bool
}

// ConnectorPatch is a partial update of a connector, limited to the fields the contract's
// ConnectorUpdate lists (which is narrower than that schema's own prose description). A nil field
// is left unchanged. Password, when non-nil, replaces the stored sealed password.
type ConnectorPatch struct {
	Name                  *string
	Host                  *string
	Port                  *int
	BindType              *BindType
	SystemID              *string
	Password              *SealedSecret
	VendorProfile         *string
	DataCodingDefault     *int
	WindowSize            *int
	ThroughputLimitPerSec *int
	TLSEnabled            *bool
	TLSConfigJSON         map[string]any
	PriorityTier          *int
	Status                *ConnectorStatus
}

// ReconnectPolicy is a partial update of a connector's auto-reconnection policy (step-128, §6.13).
// AutoReconnectEnabled is always set; the backoff knobs are optional (nil keeps the stored value).
type ReconnectPolicy struct {
	AutoReconnectEnabled bool
	InitialDelayMs       *int
	Multiplier           *float64
	MaxDelayMs           *int
	JitterPct            *int
	MaxAttempts          *int
}
