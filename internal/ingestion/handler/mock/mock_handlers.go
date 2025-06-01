package mock

import (
	"context"

	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// MockHistoricalHandler is a mock for the HistoricalHandlerInterface
type MockHistoricalHandler struct {
	mock.Mock
}

// HandleEvent mocks the HandleEvent method
func (m *MockHistoricalHandler) HandleEvent(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
	args := m.Called(ctx, eventType, metadata, rawEvent)
	return args.Error(0)
}

// MockRealtimeHandler is a mock for the RealtimeHandlerInterface
type MockRealtimeHandler struct {
	mock.Mock
}

// HandleEvent mocks the HandleEvent method
func (m *MockRealtimeHandler) HandleEvent(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
	args := m.Called(ctx, eventType, metadata, rawEvent)
	return args.Error(0)
}

// Example usage:
/*
func TestWithMockHandlers(t *testing.T) {
	// Create the mock
	mockHistHandler := new(MockHistoricalHandler)
	mockRealHandler := new(MockRealtimeHandler)

	// Setup expectations
	eventType := model.EventType("v1.historical.chats")
	metadata := &model.MessageMetadata{
		MessageID: "msg-123",
		CompanyID:  "tenant-1",
	}
	rawEvent := []byte(`{"chats":[{"id":"chat-1"}]}`)

	mockHistHandler.On("HandleEvent", mock.Anything, eventType, metadata, rawEvent).Return(nil)

	// Use the mock
	err := mockHistHandler.HandleEvent(context.Background(), eventType, metadata, rawEvent)

	// Assert
	assert.NoError(t, err)
	mockHistHandler.AssertExpectations(t)
}
*/
