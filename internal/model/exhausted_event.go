package model

import (
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm/schema"
)

// ExhaustedEvent represents a DLQ event that has reached its maximum retry count.
type ExhaustedEvent struct {
	ID              uint           `gorm:"primaryKey"`
	CreatedAt       time.Time      // Automatically set by GORM
	CompanyID       string         `gorm:"not null"`       // For tenant isolation
	SourceSubject   string         `gorm:"index;not null"` // Original subject the message came from
	LastError       string         // The last error message encountered
	RetryCount      int            // The final retry count (should be >= maxRetries)
	EventTimestamp  time.Time      `gorm:"index"`               // Timestamp from the original DLQ payload (ts field)
	DLQPayload      datatypes.JSON `gorm:"type:jsonb;not null"` // The full JSON payload from the DLQ
	OriginalPayload datatypes.JSON `gorm:"type:jsonb"`          // The extracted original payload from the DLQ message
	Resolved        bool           `gorm:"index;default:false"` // Flag to mark if the issue has been manually resolved
	ResolvedAt      *time.Time     `gorm:"index"`               // Timestamp when marked as resolved
	Notes           string         `gorm:"type:text"`           // Optional notes added during manual inspection/resolution
}

// TableName specifies the table name for the ExhaustedEvent model, respecting the Namer.
func (ExhaustedEvent) TableName(namer schema.Namer) string {
	return namer.TableName("exhausted_events") // As per the plan
}
