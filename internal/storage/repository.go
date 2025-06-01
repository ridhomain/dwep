package storage

import (
	"context"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// ChatRepo defines chat storage operations
type ChatRepo interface {
	Save(ctx context.Context, chat model.Chat) error
	Update(ctx context.Context, chat model.Chat) error
	FindChatByChatID(ctx context.Context, chatID string) (*model.Chat, error)
	FindByJID(ctx context.Context, jid string) ([]model.Chat, error)
	FindByAgentID(ctx context.Context, agentID string) ([]model.Chat, error)
	BulkUpsert(ctx context.Context, chats []model.Chat) error
	Close(ctx context.Context) error
}

// MessageRepo defines message storage operations
type MessageRepo interface {
	Save(ctx context.Context, message model.Message) error
	Update(ctx context.Context, message model.Message) error
	FindByMessageID(ctx context.Context, messageID string) (*model.Message, error)
	BulkUpsert(ctx context.Context, messages []model.Message) error

	FindByJID(ctx context.Context, jid string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error)
	FindBySender(ctx context.Context, sender string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error)
	FindByFromUser(ctx context.Context, fromUser string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error)
	FindByToUser(ctx context.Context, toUser string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error)

	Close(ctx context.Context) error
}

// ContactRepo defines contact storage operations
type ContactRepo interface {
	Save(ctx context.Context, contact model.Contact) error
	Update(ctx context.Context, contact model.Contact) error
	FindByID(ctx context.Context, id string) (*model.Contact, error)
	FindByPhone(ctx context.Context, phone string) (*model.Contact, error)
	FindByPhoneAndAgentID(ctx context.Context, phone, agentID string) (*model.Contact, error)
	BulkUpsert(ctx context.Context, contacts []model.Contact) error
	Close(ctx context.Context) error
}

// AgentRepo defines agent storage operations
type AgentRepo interface {
	Save(ctx context.Context, agent model.Agent) error
	Update(ctx context.Context, agent model.Agent) error                   // General update
	UpdateStatus(ctx context.Context, agentID string, status string) error // Specific status update
	FindByID(ctx context.Context, id int64) (*model.Agent, error)
	FindByAgentID(ctx context.Context, agentID string) (*model.Agent, error)
	FindByStatus(ctx context.Context, status string) ([]model.Agent, error)
	FindByCompanyID(ctx context.Context, companyID string) ([]model.Agent, error)
	BulkUpsert(ctx context.Context, agents []model.Agent) error
	Close(ctx context.Context) error
}

// OnboardingLogRepo defines onboarding log storage operations
type OnboardingLogRepo interface {
	Save(ctx context.Context, logEntry model.OnboardingLog) error
	FindByMessageID(ctx context.Context, messageID string) (*model.OnboardingLog, error)
	FindByPhoneNumber(ctx context.Context, phoneNumber string) ([]model.OnboardingLog, error)
	FindByAgentID(ctx context.Context, agentID string) ([]model.OnboardingLog, error)
	FindWithinTimeRange(ctx context.Context, startTimeUnix, endTimeUnix int64) ([]model.OnboardingLog, error)
	Close(ctx context.Context) error
}

// ExhaustedEventRepo defines exhausted event storage operations
type ExhaustedEventRepo interface {
	Save(ctx context.Context, event model.ExhaustedEvent) error
	Close(ctx context.Context) error // Assuming Close might be needed here too
}
