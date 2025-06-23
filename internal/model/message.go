package model

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm/schema"
)

const (
	MessageFlowIncoming = "IN"
	MessageFlowOutgoing = "OUT"
)

// Message represents a chat message with fields aligned to proto definitions
type Message struct {
	ID               int64          `json:"-" gorm:"column:id;primaryKey;autoIncrement"`
	MessageID        string         `json:"id" gorm:"column:message_id;index"`
	FromPhone        string         `json:"from_phone,omitempty" gorm:"column:from_phone;index"`
	ToPhone          string         `json:"to_phone,omitempty" gorm:"column:to_phone;index"`
	ChatID           string         `json:"chat_id,omitempty" gorm:"column:chat_id;index"`
	Jid              string         `json:"jid,omitempty" gorm:"column:jid;index"`
	Flow             string         `json:"flow,omitempty" gorm:"column:flow"`
	MessageText      string         `json:"message_text,omitempty" gorm:"column:message_text"`
	MessageUrl       string         `json:"message_url,omitempty" gorm:"column:message_url"`
	MessageType      string         `json:"message_type,omitempty" gorm:"column:message_type"`
	AgentID          string         `json:"agent_id,omitempty" gorm:"column:agent_id;index"`
	CompanyID        string         `json:"company_id,omitempty" gorm:"column:company_id"` // CompanyID is implicitly the tenant ID
	MessageObj       datatypes.JSON `json:"message_obj,omitempty" gorm:"type:jsonb;column:message_obj"`
	EditedMessageObj datatypes.JSON `json:"edited_message_obj,omitempty" gorm:"type:jsonb;column:edited_message_obj"`
	Key              datatypes.JSON `json:"key,omitempty" gorm:"type:jsonb;column:key"`
	Status           string         `json:"status,omitempty" gorm:"column:status"`
	IsDeleted        bool           `json:"is_deleted,omitempty" gorm:"column:is_deleted;default:false"`
	MessageTimestamp int64          `json:"message_timestamp,omitempty" gorm:"column:message_timestamp"`
	MessageDate      time.Time      `gorm:"column:message_date;type:date;not null"`
	CreatedAt        time.Time      `json:"created_at,omitempty" gorm:"column:created_at;autoCreateTime"`
	UpdatedAt        time.Time      `json:"updated_at,omitempty" gorm:"column:updated_at;autoUpdateTime"`
	LastMetadata     datatypes.JSON `json:"last_metadata,omitempty" gorm:"type:jsonb;column:last_metadata"`
}

// TableName specifies the base table name for GORM, respecting the Namer.
func (Message) TableName(namer schema.Namer) string {
	return namer.TableName("messages")
}

// CreateTimeFromTimestamp creates a time.Time from a Unix timestamp
func CreateTimeFromTimestamp(timestamp int64) time.Time {
	if timestamp > 0 {
		return time.Unix(timestamp, 0).UTC()
	}
	return time.Time{}
}

// GetUpdatableFields returns a list of column names that can be updated during an ON CONFLICT clause.
// Excludes primary key and creation timestamp.
func (m *Message) GetUpdatableFields() []string {
	// List all fields except 'id' and 'created_at'
	// Ensure these names match the gorm:"column:..." tags
	return []string{
		"status", "updated_at", "last_metadata", "message_text", "message_url", "message_obj", "message_type",
	}
}

func MessageUpdatableFields() []string {
	// List all fields except 'id' and 'created_at'
	// Ensure these names match the gorm:"column:..." tags
	return []string{
		"status", "last_metadata",
	}
}

// HistoryMessage represents a collection of messages from history events
type HistoryMessage struct {
	Messages []Message `json:"messages"`
}
