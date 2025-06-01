package model

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Contact represents a contact in the PostgreSQL database.
type Contact struct {
	ID                    string         `json:"id" gorm:"primaryKey;type:text"`
	PhoneNumber           string         `json:"phone_number" gorm:"type:text;uniqueIndex:uniq_agent_phone" validate:"required"`
	AgentID               string         `json:"agent_id,omitempty" gorm:"type:text;uniqueIndex:uniq_agent_phone"`
	ChatID                string         `json:"chat_id,omitempty" gorm:"column:chat_id;index;type:text"`
	Type                  string         `json:"type,omitempty" gorm:"type:text"`                  // e.g., PERSONAL, AGENT, OTHER
	CustomName            string         `json:"custom_name,omitempty" gorm:"type:text"`           // Alias or custom label
	Notes                 string         `json:"notes,omitempty" gorm:"type:text"`                 // Freeform notes
	Tags                  string         `json:"tags,omitempty" gorm:"type:text"`                  // Text of tags (comma-separated)
	CompanyID             string         `json:"company_id,omitempty" gorm:"column:company_id"`    // Company / company ID
	Avatar                string         `json:"avatar,omitempty" gorm:"type:text"`                // URL or reference to profile picture
	AssignedTo            string         `json:"assigned_to,omitempty" gorm:"index;type:text"`     // Assigned agent ID (optional)
	Pob                   string         `json:"pob,omitempty" gorm:"type:text"`                   // Place of birth
	Dob                   *time.Time     `json:"dob,omitempty" gorm:"type:date"`                   // Date of birth (pointer for nullability)
	Gender                string         `json:"gender,omitempty" gorm:"type:text;default:MALE"`   // MALE or FEMALE (default MALE)
	Origin                string         `json:"origin,omitempty" gorm:"type:text"`                // Origin source (manual, import, etc.)
	PushName              string         `json:"push_name,omitempty" gorm:"type:text"`             // Push name from WA metadata
	Status                string         `json:"status,omitempty" gorm:"type:text;default:ACTIVE"` // ACTIVE or DISABLED (default ACTIVE)
	FirstMessageID        string         `json:"first_message_id,omitempty" gorm:"type:text"`      // Message ID of first message received (nullable)
	FirstMessageTimestamp int64          `json:"first_message_timestamp,omitempty" gorm:"column:first_message_timestamp"`
	CreatedAt             time.Time      `json:"created_at,omitempty" gorm:"autoCreateTime"`
	UpdatedAt             time.Time      `json:"updated_at,omitempty" gorm:"autoUpdateTime"`
	LastMetadata          datatypes.JSON `json:"last_metadata,omitempty" gorm:"type:jsonb;column:last_metadata"`
}

// TableName specifies the table name for the Contact model, respecting the Namer.
func (Contact) TableName(namer schema.Namer) string {
	return namer.TableName("contacts")
}

// cleanPhoneForChatID removes special characters from phone number
func cleanPhoneForChatID(phone string) string {
	cleaned := strings.ReplaceAll(phone, "+", "")
	cleaned = strings.ReplaceAll(cleaned, "-", "")
	cleaned = strings.ReplaceAll(cleaned, " ", "")
	cleaned = strings.ReplaceAll(cleaned, "(", "")
	cleaned = strings.ReplaceAll(cleaned, ")", "")
	return cleaned
}

// GenerateChatID generates a chat ID from agent ID and phone number
func (c *Contact) GenerateChatID() string {
	if c.AgentID != "" && c.PhoneNumber != "" {
		cleanPhone := cleanPhoneForChatID(c.PhoneNumber)
		return fmt.Sprintf("%s_%s", c.AgentID, cleanPhone)
	}
	return ""
}

// BeforeCreate GORM hook - runs before creating a new record
func (c *Contact) BeforeCreate(tx *gorm.DB) error {
	if c.ChatID == "" {
		c.ChatID = c.GenerateChatID()
	}
	return nil
}

// BeforeUpdate GORM hook - runs before updating a record
func (c *Contact) BeforeUpdate(tx *gorm.DB) error {
	// Only regenerate if ChatID is empty and we have the required fields
	if c.ChatID == "" {
		c.ChatID = c.GenerateChatID()
	}
	return nil
}

// Update the ContactUpdateColumns function
func ContactUpdateColumns() []string {
	return []string{
		"phone_number", "type", "custom_name", "notes", "tags", "avatar", "assigned_to", "pob", "dob",
		"gender", "origin", "push_name", "status", "agent_id", "chat_id",
		"first_message_id", "first_message_timestamp", "updated_at", "last_metadata",
	}
}
