package model

import (
	"encoding/json"
	"time"
)

// --- Chat NATS Payload --- //
// UpsertChatPayload is the payload for upserting a chat in the database.
type UpsertChatPayload struct {
	ChatID                string                 `json:"chat_id,omitempty" validate:"required"`
	Jid                   string                 `json:"jid,omitempty" validate:"required"`
	CompanyID             string                 `json:"company_id,omitempty" validate:"required"`
	AgentID               string                 `json:"agent_id,omitempty" validate:"required"`
	PhoneNumber           string                 `json:"phone_number,omitempty" validate:"required"`
	PushName              string                 `json:"push_name,omitempty" validate:"omitempty"`
	CustomName            string                 `json:"custom_name,omitempty" validate:"omitempty"`
	IsGroup               bool                   `json:"is_group,omitempty" validate:"omitempty"`
	GroupName             string                 `json:"group_name,omitempty" validate:"omitempty"`
	LastMessageObj        map[string]interface{} `json:"last_message,omitempty" validate:"omitempty"`
	ConversationTimestamp int64                  `json:"conversation_timestamp,omitempty" validate:"omitempty,gte=0"`
	UnreadCount           int32                  `json:"unread_count,omitempty" validate:"omitempty"`
	NotSpam               bool                   `json:"not_spam,omitempty" validate:"omitempty"`
}

type UpdateChatPayload struct {
	ChatID                string                 `json:"chat_id,omitempty" validate:"required"`
	CompanyID             string                 `json:"company_id,omitempty" validate:"required"`
	AgentID               string                 `json:"agent_id,omitempty" validate:"required"`
	UnreadCount           int32                  `json:"unread_count,omitempty" validate:"omitempty"`
	LastMessageObj        map[string]interface{} `json:"last_message,omitempty" validate:"omitempty"`
	ConversationTimestamp int64                  `json:"conversation_timestamp,omitempty" validate:"omitempty"`
}

type HistoryChatPayload struct {
	Chats []UpsertChatPayload `json:"chats" validate:"required,dive,required"`
}

// --- Message NATS Payload --- //
type UpsertMessagePayload struct {
	MessageID        string                 `json:"message_id,omitempty" validate:"required"`
	ToPhone          string                 `json:"to_phone,omitempty" validate:"omitempty"`
	FromPhone        string                 `json:"from_phone,omitempty" validate:"omitempty"`
	ChatID           string                 `json:"chat_id,omitempty" validate:"required"`
	Jid              string                 `json:"jid,omitempty" validate:"required"`
	Flow             string                 `json:"flow,omitempty" validate:"required,oneof=IN OUT"` // Flow indicates the direction of the message (IN or OUT)
	MessageType      string                 `json:"message_type,omitempty" validate:"omitempty"`
	MessageText      string                 `json:"message_text,omitempty" validate:"omitempty"`
	MessageUrl       string                 `json:"message_url,omitempty" validate:"omitempty"`
	CompanyID        string                 `json:"company_id,omitempty" validate:"required"`
	AgentID          string                 `json:"agent_id,omitempty" validate:"required"`
	Key              *KeyPayload            `json:"key,omitempty" validate:"omitempty"`
	MessageObj       map[string]interface{} `json:"message_obj,omitempty" validate:"omitempty"`
	Status           string                 `json:"status,omitempty" validate:"omitempty"`
	MessageTimestamp int64                  `json:"message_timestamp,omitempty" validate:"omitempty"`
	Campaign         *CampaignData          `json:"campaign,omitempty" validate:"omitempty"`
}

// CampaignData holds campaign-specific information
type CampaignData struct {
	IsCampaign bool   `json:"is_campaign"`
	Tags       string `json:"tags,omitempty"`
	AssignedTo string `json:"assigned_to,omitempty"`
	Origin     string `json:"origin,omitempty"`
}

type KeyPayload struct {
	ID        string `json:"id,omitempty" validate:"required"`
	FromMe    bool   `json:"fromMe,omitempty" validate:"omitempty"`
	RemoteJid string `json:"remoteJid,omitempty" validate:"omitempty"`
}

type HistoryMessagePayload struct {
	Messages    []UpsertMessagePayload `json:"messages" validate:"required,dive,required"`
	IsLastBatch bool                   `json:"is_last_batch,omitempty"`
	AgentID     string                 `json:"agent_id,omitempty"`
}

type UpdateMessagePayload struct {
	MessageID        string                 `json:"message_id,omitempty" validate:"required"`
	CompanyID        string                 `json:"company_id,omitempty" validate:"required"`
	Status           string                 `json:"status,omitempty" validate:"omitempty"`
	EditedMessageObj map[string]interface{} `json:"edited_message_obj,omitempty" validate:"omitempty"`
	IsDeleted        bool                   `json:"is_deleted,omitempty" validate:"omitempty"`
	MessageUrl       string                 `json:"message_url,omitempty" validate:"omitempty"`
}

// --- Agent NATS Payload --- //
type UpsertAgentPayload struct {
	AgentID     string `json:"agent_id,omitempty" validate:"required"`
	CompanyID   string `json:"company_id,omitempty" validate:"required"`
	QRCode      string `json:"qr_code,omitempty" validate:"omitempty"`
	Status      string `json:"status,omitempty" validate:"omitempty"`
	AgentName   string `json:"agent_name,omitempty" validate:"omitempty"`
	HostName    string `json:"host_name,omitempty" validate:"omitempty"`
	Version     string `json:"version,omitempty" validate:"omitempty"`
	PhoneNumber string `json:"phone_number,omitempty" validate:"omitempty"`
}

// --- Contact NATS Payload --- //
type UpsertContactPayload struct {
	ChatID      string `json:"chat_id,omitempty" validate:"required"`
	PhoneNumber string `json:"phone_number,omitempty" validate:"required"`
	CompanyID   string `json:"company_id,omitempty" validate:"required"`
	AgentID     string `json:"agent_id,omitempty" validate:"required"`
	PushName    string `json:"push_name,omitempty" validate:"required"` // Using pointers to allow partial updates
}

// UNUSED FOR NOW
type UpdateContactPayload struct {
	PhoneNumber string  `json:"phone_number,omitempty" validate:"required"`
	CompanyID   string  `json:"company_id,omitempty" validate:"required"`
	AgentID     string  `json:"agent_id,omitempty" validate:"required"`
	PushName    *string `json:"push_name,omitempty" validate:"required"` // Using pointers to allow partial updates
}

// --- History Contact NATS Payload --- //
type HistoryContactPayload struct {
	Contacts []UpsertContactPayload `json:"contacts" validate:"required,dive,required"`
}

// --- DLQ Payload --- //
// DLQPayload represents the structure of messages sent to the Dead Letter Queue.
type DLQPayload struct {
	SourceSubject   string          `json:"source_subject"`          // The original subject the message was published to
	Company         string          `json:"company"`                 // The company ID associated with the message
	OriginalPayload json.RawMessage `json:"original_payload"`        // The raw JSON payload of the original message
	Error           string          `json:"error"`                   // The full error message encountered during processing
	ErrorType       string          `json:"error_type"`              // Type of error ('fatal', 'retryable', 'unknown')
	RetryCount      uint64          `json:"retry_count"`             // How many times delivery was attempted (NumDelivered from NATS metadata)
	MaxRetry        int             `json:"max_retry"`               // The configured maximum delivery attempts for the consumer
	NextRetryAt     *time.Time      `json:"next_retry_at,omitempty"` // Timestamp for the next scheduled retry attempt (set by DLQ worker)
	Timestamp       time.Time       `json:"ts"`                      // Timestamp when the message was sent to the DLQ
}
