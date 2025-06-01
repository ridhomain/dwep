package mock

import (
	"context"

	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// MockHistoricalService is a mock for the HistoricalService interface
type MockHistoricalService struct {
	mock.Mock
}

// ProcessHistoricalChats mocks the ProcessHistoricalChats method
func (m *MockHistoricalService) ProcessHistoricalChats(ctx context.Context, chats []model.UpsertChatPayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, chats, metadata)
	return args.Error(0)
}

// ProcessHistoricalMessages mocks the ProcessHistoricalMessages method
func (m *MockHistoricalService) ProcessHistoricalMessages(ctx context.Context, messages []model.UpsertMessagePayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, messages, metadata)
	return args.Error(0)
}

// ProcessHistoricalContacts mocks the ProcessHistoricalContacts method
func (m *MockHistoricalService) ProcessHistoricalContacts(ctx context.Context, contacts []model.UpsertContactPayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, contacts, metadata)
	return args.Error(0)
}

// CreateOnboardingLogIfNeeded mocks the CreateOnboardingLogIfNeeded method
func (m *MockHistoricalService) CreateOnboardingLogIfNeeded(ctx context.Context, companyID, chatJID string) {
	m.Called(ctx, companyID, chatJID)
	// This method has no return value
}

// MockRealtimeService is a mock for the RealtimeService interface
type MockRealtimeService struct {
	mock.Mock
}

// UpsertChat mocks the UpsertChat method
func (m *MockRealtimeService) UpsertChat(ctx context.Context, chat model.UpsertChatPayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, chat, metadata)
	return args.Error(0)
}

// UpdateChat mocks the UpdateChat method
func (m *MockRealtimeService) UpdateChat(ctx context.Context, chat model.UpdateChatPayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, chat, metadata)
	return args.Error(0)
}

// UpsertMessage mocks the UpsertMessage method
func (m *MockRealtimeService) UpsertMessage(ctx context.Context, message model.UpsertMessagePayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, message, metadata)
	return args.Error(0)
}

// UpdateMessage mocks the UpdateMessage method
func (m *MockRealtimeService) UpdateMessage(ctx context.Context, message model.UpdateMessagePayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, message, metadata)
	return args.Error(0)
}

// UpsertContact mocks the UpsertContact method
func (m *MockRealtimeService) UpsertContact(ctx context.Context, contact model.UpsertContactPayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, contact, metadata)
	return args.Error(0)
}

// UpdateContact mocks the UpdateContact method
func (m *MockRealtimeService) UpdateContact(ctx context.Context, contact model.UpdateContactPayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, contact, metadata)
	return args.Error(0)
}

// UpsertAgent mocks the UpsertAgent method
func (m *MockRealtimeService) UpsertAgent(ctx context.Context, agent model.UpsertAgentPayload, metadata *model.LastMetadata) error {
	args := m.Called(ctx, agent, metadata)
	return args.Error(0)
}

// Example usage:
/*
func TestHistoricalHandlerWithMockService(t *testing.T) {
	// Create the mock service
	mockService := new(MockHistoricalService)

	// Create handler with mock service
	handler := handler.NewHistoricalHandler(mockService)

	// Setup test data
	ctx := context.Background()
	eventType := model.V1HistoricalChats
	metadata := &model.MessageMetadata{
		MessageID: "msg-123",
		CompanyID:  "tenant-1",
	}
	rawEvent := []byte(`{"chats":[{"id":"chat-1","cluster":"tenant-1"}]}`)

	// Setup expectations on the mock service
	mockService.On("ProcessHistoricalChats", mock.Anything, mock.AnythingOfType("[]model.UpsertChatPayload"), mock.AnythingOfType("*model.LastMetadata")).Return(nil)

	// Call the handler
	err := handler.HandleEvent(ctx, eventType, metadata, rawEvent)

	// Assert
	assert.NoError(t, err)
	mockService.AssertExpectations(t)
}
*/
