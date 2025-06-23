package model

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm/schema"
)

// Contact represents a contact in the PostgreSQL database.
type Contact struct {
	ID                    string         `json:"id" gorm:"primaryKey;type:text"`
	PhoneNumber           string         `json:"phone_number" gorm:"type:text" validate:"required"`
	AgentID               string         `json:"agent_id,omitempty" gorm:"type:text"`
	ChatID                string         `json:"chat_id,omitempty" gorm:"column:chat_id;uniqueIndex;type:text" validate:"required"`
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

// Update the ContactUpdateColumns function
func ContactUpdateColumns() []string {
	return []string{
		"push_name",
	}
}
