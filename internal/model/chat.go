package model

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm/schema"
)

// Chat represents a chat session
type Chat struct {
	ID                    int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	ChatID                string         `json:"id" gorm:"column:chat_id;uniqueIndex"`
	Jid                   string         `json:"jid,omitempty" gorm:"column:jid;index"`
	PushName              string         `json:"push_name,omitempty" gorm:"column:push_name"`
	IsGroup               bool           `json:"is_group,omitempty" gorm:"column:is_group"`
	GroupName             string         `json:"group_name,omitempty" gorm:"column:group_name"`
	UnreadCount           int32          `json:"unread_count,omitempty" gorm:"column:unread_count"`
	LastMessageObj        interface{}    `json:"last_message,omitempty" gorm:"type:jsonb;column:last_message"`
	ConversationTimestamp int64          `json:"conversation_timestamp,omitempty" gorm:"column:conversation_timestamp"`
	NotSpam               bool           `json:"not_spam,omitempty" gorm:"column:not_spam"`
	AgentID               string         `json:"agent_id,omitempty" gorm:"column:agent_id;index"`
	CompanyID             string         `json:"company_id,omitempty" gorm:"column:company_id"`
	PhoneNumber           string         `json:"phone_number,omitempty" gorm:"column:phone_number"` // Renamed from phone_number in PRD
	LastMetadata          datatypes.JSON `json:"last_metadata,omitempty" gorm:"type:jsonb;column:last_metadata"`
	CreatedAt             time.Time      `json:"created_at,omitempty" gorm:"column:created_at;autoCreateTime"`
	UpdatedAt             time.Time      `json:"updated_at,omitempty" gorm:"column:updated_at;autoUpdateTime"`
}

// TableName specifies the base table name for GORM migrations, respecting the Namer.
func (Chat) TableName(namer schema.Namer) string {
	return namer.TableName("chats")
}

// GetUpdatableFields returns a list of column names that can be updated during an ON CONFLICT clause.
func (c *Chat) GetUpdatableFields() []string {
	// Excludes id, created_at, chat_id, company_id (part of conflict target)
	return []string{
		"jid", "push_name", "is_group", "group_name",
		"unread_count", "last_message", "conversation_timestamp",
		"not_spam", "agent_id", "phone_number", "last_metadata", "updated_at",
	}
}

func ChatUpdatableFields() []string {
	// Excludes id, created_at, chat_id, company_id (part of conflict target)
	return []string{
		"jid", "is_group", "group_name", "unread_count", "last_message", "conversation_timestamp",
		"not_spam", "agent_id", "phone_number", "last_metadata", "updated_at",
	}
}
