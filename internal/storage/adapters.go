package storage

import (
	"context"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// ChatRepoAdapter adapts the PostgresRepo to the ChatRepo interface
type ChatRepoAdapter struct {
	postgres *PostgresRepo
}

// NewChatRepoAdapter creates a new chat repository adapter
func NewChatRepoAdapter(postgres *PostgresRepo) ChatRepo {
	return &ChatRepoAdapter{postgres: postgres}
}

// Save saves a chat
func (a *ChatRepoAdapter) Save(ctx context.Context, chat model.Chat) error {
	return a.postgres.SaveChat(ctx, chat)
}

// Update updates a chat
func (a *ChatRepoAdapter) Update(ctx context.Context, chat model.Chat) error {
	return a.postgres.UpdateChat(ctx, chat)
}

// FindChatByChatID finds a chat by ID
func (a *ChatRepoAdapter) FindChatByChatID(ctx context.Context, chatID string) (*model.Chat, error) {
	return a.postgres.FindChatByChatID(ctx, chatID)
}

// FindByJID finds chats by JID
func (a *ChatRepoAdapter) FindByJID(ctx context.Context, jid string) ([]model.Chat, error) {
	return a.postgres.FindChatsByJID(ctx, jid)
}

// FindByAgentID finds chats by Agent ID
func (a *ChatRepoAdapter) FindByAgentID(ctx context.Context, agentID string) ([]model.Chat, error) {
	return a.postgres.FindChatsByAgentID(ctx, agentID)
}

// BulkUpsert performs a bulk upsert of chats
func (a *ChatRepoAdapter) BulkUpsert(ctx context.Context, chats []model.Chat) error {
	return a.postgres.BulkUpsertChats(ctx, chats)
}

func (a *ChatRepoAdapter) Close(ctx context.Context) error {
	return a.postgres.Close(ctx)
}

// MessageRepoAdapter adapts the PostgresRepo to the MessageRepo interface
type MessageRepoAdapter struct {
	postgres *PostgresRepo
}

// NewMessageRepoAdapter creates a new message repository adapter
func NewMessageRepoAdapter(postgres *PostgresRepo) MessageRepo {
	return &MessageRepoAdapter{postgres: postgres}
}

func (a *MessageRepoAdapter) FindByJID(ctx context.Context, jid string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	return a.postgres.FindMessagesByJID(ctx, jid, startDate, endDate, limit, offset)
}

func (a *MessageRepoAdapter) FindBySender(ctx context.Context, sender string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	return a.postgres.FindMessagesBySender(ctx, sender, startDate, endDate, limit, offset)
}

func (a *MessageRepoAdapter) FindByFromPhone(ctx context.Context, FromPhone string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	return a.postgres.FindMessagesByFromPhone(ctx, FromPhone, startDate, endDate, limit, offset)
}

func (a *MessageRepoAdapter) FindByToPhone(ctx context.Context, ToPhone string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	return a.postgres.FindMessagesByToPhone(ctx, ToPhone, startDate, endDate, limit, offset)
}

// Save saves a message
func (a *MessageRepoAdapter) Save(ctx context.Context, message model.Message) error {
	return a.postgres.SaveMessage(ctx, message)
}

// Update updates a message
func (a *MessageRepoAdapter) Update(ctx context.Context, message model.Message) error {
	return a.postgres.UpdateMessage(ctx, message)
}

// FindByMessageID finds a message by ID
func (a *MessageRepoAdapter) FindByMessageID(ctx context.Context, messageID string) (*model.Message, error) {
	return a.postgres.FindMessageByMessageID(ctx, messageID)
}

// FindFirstMessageByPhoneNumber finds first message by phone number
func (a *MessageRepoAdapter) FindFirstMessageByPhoneNumber(ctx context.Context, phoneNumber string) (*model.Message, error) {
	return a.postgres.FindFirstMessageByPhoneNumber(ctx, phoneNumber)
}

// BulkUpsert performs a bulk upsert of messages
func (a *MessageRepoAdapter) BulkUpsert(ctx context.Context, messages []model.Message) error {
	return a.postgres.BulkUpsertMessages(ctx, messages)
}

func (a *MessageRepoAdapter) Close(ctx context.Context) error {
	return a.postgres.Close(ctx)
}

// ContactRepoAdapter adapts the PostgresRepo to the ContactRepo interface
type ContactRepoAdapter struct {
	postgres *PostgresRepo
}

func (a *ContactRepoAdapter) Close(ctx context.Context) error {
	return a.postgres.Close(ctx)
}

// NewContactRepoAdapter creates a new contact repository adapter
func NewContactRepoAdapter(postgres *PostgresRepo) ContactRepo {
	return &ContactRepoAdapter{postgres: postgres}
}

// Save saves a contact
func (a *ContactRepoAdapter) Save(ctx context.Context, contact model.Contact) error {
	return a.postgres.SaveContact(ctx, contact)
}

// Update updates a contact
func (a *ContactRepoAdapter) Update(ctx context.Context, contact model.Contact) error {
	return a.postgres.UpdateContact(ctx, contact)
}

// FindByID finds a contact by ID
func (a *ContactRepoAdapter) FindByID(ctx context.Context, id string) (*model.Contact, error) {
	return a.postgres.FindContactByID(ctx, id)
}

// FindByPhone finds a contact by phone number
func (a *ContactRepoAdapter) FindByPhone(ctx context.Context, phone string) (*model.Contact, error) {
	return a.postgres.FindContactByPhone(ctx, phone)
}

// FindByPhoneAndAgentID finds a contact by phone number and agent ID
func (a *ContactRepoAdapter) FindByPhoneAndAgentID(ctx context.Context, phone, agentID string) (*model.Contact, error) {
	return a.postgres.FindContactByPhoneAndAgentID(ctx, phone, agentID)
}

// BulkUpsert performs a bulk upsert of contacts
func (a *ContactRepoAdapter) BulkUpsert(ctx context.Context, contacts []model.Contact) error {
	return a.postgres.BulkUpsertContacts(ctx, contacts)
}

// AgentRepoAdapter adapts the PostgresRepo to the AgentRepo interface
type AgentRepoAdapter struct {
	postgres *PostgresRepo
}

// NewAgentRepoAdapter creates a new agent repository adapter
func NewAgentRepoAdapter(postgres *PostgresRepo) AgentRepo {
	return &AgentRepoAdapter{postgres: postgres}
}

// Save saves an agent
func (a *AgentRepoAdapter) Save(ctx context.Context, agent model.Agent) error {
	return a.postgres.SaveAgent(ctx, agent)
}

// Update updates an agent
func (a *AgentRepoAdapter) Update(ctx context.Context, agent model.Agent) error {
	return a.postgres.UpdateAgent(ctx, agent)
}

// UpdateStatus updates an agent's status
func (a *AgentRepoAdapter) UpdateStatus(ctx context.Context, agentID string, status string) error {
	return a.postgres.UpdateAgentStatus(ctx, agentID, status)
}

// FindByID finds an agent by internal ID
func (a *AgentRepoAdapter) FindByID(ctx context.Context, id int64) (*model.Agent, error) {
	return a.postgres.FindAgentByID(ctx, id)
}

// FindByAgentID finds an agent by agent ID
func (a *AgentRepoAdapter) FindByAgentID(ctx context.Context, agentID string) (*model.Agent, error) {
	return a.postgres.FindAgentByAgentID(ctx, agentID)
}

// FindByStatus finds agents by status
func (a *AgentRepoAdapter) FindByStatus(ctx context.Context, status string) ([]model.Agent, error) {
	return a.postgres.FindAgentsByStatus(ctx, status)
}

// FindByCompanyID finds agents by company ID
func (a *AgentRepoAdapter) FindByCompanyID(ctx context.Context, companyID string) ([]model.Agent, error) {
	return a.postgres.FindAgentsByCompanyID(ctx, companyID)
}

// BulkUpsert performs a bulk upsert of agents
func (a *AgentRepoAdapter) BulkUpsert(ctx context.Context, agents []model.Agent) error {
	return a.postgres.BulkUpsertAgents(ctx, agents)
}

// Close closes the repository
func (a *AgentRepoAdapter) Close(ctx context.Context) error {
	return a.postgres.Close(ctx)
}

// OnboardingLogRepoAdapter adapts the PostgresRepo to the OnboardingLogRepo interface
type OnboardingLogRepoAdapter struct {
	postgres *PostgresRepo
}

// NewOnboardingLogRepoAdapter creates a new onboarding log repository adapter
func NewOnboardingLogRepoAdapter(postgres *PostgresRepo) OnboardingLogRepo {
	return &OnboardingLogRepoAdapter{postgres: postgres}
}

// Save saves an onboarding log entry
func (a *OnboardingLogRepoAdapter) Save(ctx context.Context, logEntry model.OnboardingLog) error {
	return a.postgres.SaveOnboardingLog(ctx, logEntry)
}

// FindByMessageID finds an onboarding log entry by message ID
func (a *OnboardingLogRepoAdapter) FindByMessageID(ctx context.Context, messageID string) (*model.OnboardingLog, error) {
	return a.postgres.FindOnboardingLogByMessageID(ctx, messageID)
}

// FindByPhoneNumber finds onboarding log entries by phone number
func (a *OnboardingLogRepoAdapter) FindByPhoneNumber(ctx context.Context, phoneNumber string) ([]model.OnboardingLog, error) {
	return a.postgres.FindOnboardingLogsByPhoneNumber(ctx, phoneNumber)
}

// FindByAgentID finds onboarding log entries by agent ID
func (a *OnboardingLogRepoAdapter) FindByAgentID(ctx context.Context, agentID string) ([]model.OnboardingLog, error) {
	return a.postgres.FindOnboardingLogsByAgentID(ctx, agentID)
}

// FindWithinTimeRange finds onboarding logs within a time range
func (a *OnboardingLogRepoAdapter) FindWithinTimeRange(ctx context.Context, startTimeUnix, endTimeUnix int64) ([]model.OnboardingLog, error) {
	return a.postgres.FindOnboardingLogsWithinTimeRange(ctx, startTimeUnix, endTimeUnix)
}

// Close closes the repository
func (a *OnboardingLogRepoAdapter) Close(ctx context.Context) error {
	return a.postgres.Close(ctx)
}

// --- ExhaustedEventRepo Adapter ---

// ExhaustedEventRepoAdapter adapts the PostgresRepo to the ExhaustedEventRepo interface
type ExhaustedEventRepoAdapter struct {
	postgres *PostgresRepo
}

// NewExhaustedEventRepoAdapter creates a new exhausted event repository adapter
func NewExhaustedEventRepoAdapter(postgres *PostgresRepo) ExhaustedEventRepo {
	return &ExhaustedEventRepoAdapter{postgres: postgres}
}

// Save saves an exhausted event
func (a *ExhaustedEventRepoAdapter) Save(ctx context.Context, event model.ExhaustedEvent) error {
	return a.postgres.SaveExhaustedEvent(ctx, event)
}

// Close closes the repository
func (a *ExhaustedEventRepoAdapter) Close(ctx context.Context) error {
	return a.postgres.Close(ctx)
}

// Ensure adapters implement the interfaces
var _ ChatRepo = (*ChatRepoAdapter)(nil)
var _ MessageRepo = (*MessageRepoAdapter)(nil)
var _ ContactRepo = (*ContactRepoAdapter)(nil)
var _ AgentRepo = (*AgentRepoAdapter)(nil)
var _ OnboardingLogRepo = (*OnboardingLogRepoAdapter)(nil)
var _ ExhaustedEventRepo = (*ExhaustedEventRepoAdapter)(nil)
