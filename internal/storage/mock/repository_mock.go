package mock

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// --- Repository Mock (Combined Interface) ---

// RepositoryMock mocks the combined Repository interface
type RepositoryMock struct {
	mock.Mock
	ChatRepoMock           // Embed ChatRepoMock
	MessageRepoMock        // Embed MessageRepoMock
	ContactRepoMock        // Embed ContactRepoMock
	AgentRepoMock          // Embed AgentRepoMock
	OnboardingLogRepoMock  // Embed OnboardingLogRepoMock
	ExhaustedEventRepoMock // Embed ExhaustedEventRepoMock
}

// Close mocks the Close method
func (m *RepositoryMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

// --- ChatRepo Mock ---

// ChatRepoMock mocks the ChatRepo interface
type ChatRepoMock struct {
	mock.Mock
}

// Save mocks the Save method
func (m *ChatRepoMock) Save(ctx context.Context, chat model.Chat) error {
	args := m.Called(ctx, chat)
	return args.Error(0)
}

// Update mocks the Update method
func (m *ChatRepoMock) Update(ctx context.Context, chat model.Chat) error {
	args := m.Called(ctx, chat)
	return args.Error(0)
}

// FindByID mocks the FindByID method
func (m *ChatRepoMock) FindByID(ctx context.Context, id string) (*model.Chat, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Chat), args.Error(1)
}

// Added FindByChatID mock method
func (m *ChatRepoMock) FindChatByChatID(ctx context.Context, chatID string) (*model.Chat, error) {
	args := m.Called(ctx, chatID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Chat), args.Error(1)
}

// BulkUpsert mocks the BulkUpsert method
func (m *ChatRepoMock) BulkUpsert(ctx context.Context, chats []model.Chat) error {
	args := m.Called(ctx, chats)
	return args.Error(0)
}

func (m *ChatRepoMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// FindByJID mocks the FindByJID method for ChatRepo
func (m *ChatRepoMock) FindByJID(ctx context.Context, jid string) ([]model.Chat, error) {
	args := m.Called(ctx, jid)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Chat), args.Error(1)
}

// FindByAgentID mocks the FindByAgentID method for ChatRepo
func (m *ChatRepoMock) FindByAgentID(ctx context.Context, agentID string) ([]model.Chat, error) {
	args := m.Called(ctx, agentID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Chat), args.Error(1)
}

// --- MessageRepo Mock ---

// MessageRepoMock mocks the MessageRepo interface
type MessageRepoMock struct {
	mock.Mock
}

// Save mocks the Save method
func (m *MessageRepoMock) Save(ctx context.Context, message model.Message) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}

// Update mocks the Update method
func (m *MessageRepoMock) Update(ctx context.Context, message model.Message) error {
	args := m.Called(ctx, message)
	return args.Error(0)
}

// FindByID mocks the FindByID method
func (m *MessageRepoMock) FindByID(ctx context.Context, id string) (*model.Message, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Message), args.Error(1)
}

// FindByMessageID Added FindByMessageID mock method
func (m *MessageRepoMock) FindByMessageID(ctx context.Context, messageID string) (*model.Message, error) {
	args := m.Called(ctx, messageID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Message), args.Error(1)
}

// FindByChatID mocks the FindByChatID method
func (m *MessageRepoMock) FindByChatID(ctx context.Context, chatID string, limit, offset int) ([]model.Message, error) {
	args := m.Called(ctx, chatID, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Message), args.Error(1)
}

// BulkUpsert mocks the BulkUpsert method
func (m *MessageRepoMock) BulkUpsert(ctx context.Context, messages []model.Message) error {
	args := m.Called(ctx, messages)
	return args.Error(0)
}

func (m *MessageRepoMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// FindByJID mocks the FindByJID method
func (m *MessageRepoMock) FindByJID(ctx context.Context, jid string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	args := m.Called(ctx, jid, startDate, endDate, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Message), args.Error(1)
}

// FindBySender mocks the FindBySender method
func (m *MessageRepoMock) FindBySender(ctx context.Context, sender string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	args := m.Called(ctx, sender, startDate, endDate, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Message), args.Error(1)
}

// FindByFromUser mocks the FindByFromUser method
func (m *MessageRepoMock) FindByFromUser(ctx context.Context, fromUser string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	args := m.Called(ctx, fromUser, startDate, endDate, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Message), args.Error(1)
}

// FindByToUser mocks the FindByToUser method
func (m *MessageRepoMock) FindByToUser(ctx context.Context, toUser string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	args := m.Called(ctx, toUser, startDate, endDate, limit, offset)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Message), args.Error(1)
}

// --- ContactRepo Mock ---

// ContactRepoMock mocks the ContactRepo interface
type ContactRepoMock struct {
	mock.Mock
}

// Save mocks the Save method
func (m *ContactRepoMock) Save(ctx context.Context, contact model.Contact) error {
	args := m.Called(ctx, contact)
	return args.Error(0)
}

// Update mocks the Update method
func (m *ContactRepoMock) Update(ctx context.Context, contact model.Contact) error {
	args := m.Called(ctx, contact)
	return args.Error(0)
}

// FindByID mocks the FindByID method
func (m *ContactRepoMock) FindByID(ctx context.Context, id string) (*model.Contact, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Contact), args.Error(1)
}

// FindByPhone mocks the FindByPhone method
func (m *ContactRepoMock) FindByPhone(ctx context.Context, phone string) (*model.Contact, error) {
	args := m.Called(ctx, phone)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Contact), args.Error(1)
}

// FindByPhoneAndAgentID mocks the FindByPhoneAndAgentID method
func (m *ContactRepoMock) FindByPhoneAndAgentID(ctx context.Context, phone, agentID string) (*model.Contact, error) {
	args := m.Called(ctx, phone, agentID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Contact), args.Error(1)
}

// FindByEmail mocks the FindByEmail method
func (m *ContactRepoMock) FindByEmail(ctx context.Context, email string) (*model.Contact, error) {
	args := m.Called(ctx, email)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Contact), args.Error(1)
}

// BulkUpsert mocks the BulkUpsert method
func (m *ContactRepoMock) BulkUpsert(ctx context.Context, contacts []model.Contact) error {
	args := m.Called(ctx, contacts)
	return args.Error(0)
}

func (m *ContactRepoMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// FindByAgentID mocks the FindByAgentID method for ContactRepo
func (m *ContactRepoMock) FindByAgentID(ctx context.Context, agentID string) ([]model.Contact, error) {
	args := m.Called(ctx, agentID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Contact), args.Error(1)
}

// --- AgentRepo Mock ---

// AgentRepoMock mocks the AgentRepo interface
type AgentRepoMock struct {
	mock.Mock
}

// Save mocks the Save method
func (m *AgentRepoMock) Save(ctx context.Context, agent model.Agent) error {
	args := m.Called(ctx, agent)
	return args.Error(0)
}

// Update mocks the Update method
func (m *AgentRepoMock) Update(ctx context.Context, agent model.Agent) error {
	args := m.Called(ctx, agent)
	return args.Error(0)
}

// UpdateStatus mocks the UpdateStatus method
func (m *AgentRepoMock) UpdateStatus(ctx context.Context, agentID string, status string) error {
	args := m.Called(ctx, agentID, status)
	return args.Error(0)
}

// FindByID mocks the FindByID method
func (m *AgentRepoMock) FindByID(ctx context.Context, id int64) (*model.Agent, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Agent), args.Error(1)
}

// FindByAgentID mocks the FindByAgentID method
func (m *AgentRepoMock) FindByAgentID(ctx context.Context, agentID string) (*model.Agent, error) {
	args := m.Called(ctx, agentID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Agent), args.Error(1)
}

// FindByStatus mocks the FindByStatus method
func (m *AgentRepoMock) FindByStatus(ctx context.Context, status string) ([]model.Agent, error) {
	args := m.Called(ctx, status)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Agent), args.Error(1)
}

// FindByCompanyID mocks the FindByCompanyID method
func (m *AgentRepoMock) FindByCompanyID(ctx context.Context, companyID string) ([]model.Agent, error) {
	args := m.Called(ctx, companyID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.Agent), args.Error(1)
}

// BulkUpsert mocks the BulkUpsert method
func (m *AgentRepoMock) BulkUpsert(ctx context.Context, agents []model.Agent) error {
	args := m.Called(ctx, agents)
	return args.Error(0)
}

// Close mocks the Close method
func (m *AgentRepoMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// --- OnboardingLogRepo Mock ---

// OnboardingLogRepoMock mocks the OnboardingLogRepo interface
type OnboardingLogRepoMock struct {
	mock.Mock
}

// Save mocks the Save method
func (m *OnboardingLogRepoMock) Save(ctx context.Context, logEntry model.OnboardingLog) error {
	args := m.Called(ctx, logEntry)
	return args.Error(0)
}

// FindByMessageID mocks the FindByMessageID method
func (m *OnboardingLogRepoMock) FindByMessageID(ctx context.Context, messageID string) (*model.OnboardingLog, error) {
	args := m.Called(ctx, messageID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.OnboardingLog), args.Error(1)
}

// FindByPhoneNumber mocks the FindByPhoneNumber method
func (m *OnboardingLogRepoMock) FindByPhoneNumber(ctx context.Context, phoneNumber string) ([]model.OnboardingLog, error) {
	args := m.Called(ctx, phoneNumber)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.OnboardingLog), args.Error(1)
}

// FindByAgentID mocks the FindByAgentID method
func (m *OnboardingLogRepoMock) FindByAgentID(ctx context.Context, agentID string) ([]model.OnboardingLog, error) {
	args := m.Called(ctx, agentID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.OnboardingLog), args.Error(1)
}

// FindWithinTimeRange mocks the FindWithinTimeRange method
func (m *OnboardingLogRepoMock) FindWithinTimeRange(ctx context.Context, startTimeUnix, endTimeUnix int64) ([]model.OnboardingLog, error) {
	args := m.Called(ctx, startTimeUnix, endTimeUnix)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]model.OnboardingLog), args.Error(1)
}

// Close mocks the Close method
func (m *OnboardingLogRepoMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// --- ExhaustedEventRepo Mock ---

// ExhaustedEventRepoMock mocks the ExhaustedEventRepo interface
type ExhaustedEventRepoMock struct {
	mock.Mock
}

// Save mocks the Save method for ExhaustedEventRepo
func (m *ExhaustedEventRepoMock) Save(ctx context.Context, event model.ExhaustedEvent) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

// Close mocks the Close method for ExhaustedEventRepo
func (m *ExhaustedEventRepoMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
