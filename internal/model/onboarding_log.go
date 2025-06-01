package model

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm/schema"
)

// OnboardingLog represents the onboarding_log table structure, recording when contacts are automatically created.
type OnboardingLog struct {
	// ID is the internal database primary key.
	ID int64 `json:"-" gorm:"primaryKey;autoIncrement"`
	// MessageID is the ID of the message that triggered the contact onboarding.
	MessageID string `json:"message_id" gorm:"column:message_id;index" validate:"required"`
	// AgentID is the agent associated with the incoming message.
	AgentID string `json:"agent_id" gorm:"column:agent_id;index"`
	// CompanyID identifies the company/tenant this log entry belongs to.
	CompanyID string `json:"company_id" gorm:"column:company_id"` // CompanyID is implicitly the tenant ID
	// PhoneNumber is the phone number of the contact that was onboarded (from the message 'from' field).
	PhoneNumber string `json:"phone_number" gorm:"column:phone_number;index" validate:"required"`
	// Timestamp is the Unix timestamp indicating when the onboarding occurred.
	Timestamp int64 `json:"timestamp" gorm:"column:timestamp" validate:"required,gte=0"`
	// CreatedAt is the timestamp when the log record was created.
	CreatedAt    time.Time      `json:"created_at" gorm:"column:created_at;autoCreateTime"` // Added for consistency
	LastMetadata datatypes.JSON `json:"last_metadata,omitempty" gorm:"type:jsonb;column:last_metadata"`
}

// TableName specifies the table name for GORM, respecting the Namer.
func (OnboardingLog) TableName(namer schema.Namer) string {
	return namer.TableName("onboarding_log")
}
