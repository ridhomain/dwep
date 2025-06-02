package model

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm/schema"
)

// Agent represents the agents table structure, containing information about connected WhatsApp agents.
type Agent struct {
	// ID is the internal database primary key.
	ID int64 `json:"-" gorm:"primaryKey;autoIncrement"`
	// AgentID is the unique identifier for the agent (e.g., from the WA client).
	AgentID string `json:"agent_id" gorm:"column:agent_id;uniqueIndex" validate:"required"`
	// QRCode is the QR code content used for linking/pairing the agent, if applicable.
	QRCode string `json:"qr_code,omitempty" gorm:"column:qr_code"`
	// Status indicates the current connection status of the agent (e.g., 'connected', 'disconnected').
	Status string `json:"status,omitempty" gorm:"column:status"`
	// AgentName is a user-defined custom label or name for the agent.
	AgentName string `json:"agent_name,omitempty" gorm:"column:agent_name"`
	// PhoneNumber is a number that registered for the agent.
	PhoneNumber string `json:"phone_number,omitempty" gorm:"column:phone_number"`
	// HostName is the name of the device or host machine running the agent instance.
	HostName string `json:"host_name,omitempty" gorm:"column:host_name"`
	// Version stores the version information of the agent software.
	Version string `json:"version,omitempty" gorm:"column:version"`
	// CompanyID identifies the company/tenant this agent belongs to.
	CompanyID string `json:"company_id,omitempty" gorm:"column:company_id"` // CompanyID is implicitly the tenant ID
	// CreatedAt is the timestamp when the agent record was first created.
	CreatedAt time.Time `json:"created_at,omitempty" gorm:"column:created_at;autoCreateTime"`
	// UpdatedAt is the timestamp when the agent record was last updated.
	UpdatedAt    time.Time      `json:"updated_at,omitempty" gorm:"column:updated_at;autoUpdateTime"`
	LastMetadata datatypes.JSON `json:"last_metadata,omitempty" gorm:"type:jsonb;column:last_metadata"`
}

// TableName specifies the table name for GORM.
func (Agent) TableName(namer schema.Namer) string {
	return namer.TableName("agents")
}

// AgentUpdateColumns returns a list of column names that are allowed to be updated during an upsert operation.
// Excludes primary key, agent_id, company_id, and created_at.
func AgentUpdateColumns() []string {
	return []string{
		"qr_code",
		"status",
		"agent_name",
		"host_name",
		"version",
		"updated_at",
		"phone_number",
	}
}
