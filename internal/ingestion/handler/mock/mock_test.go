package mock

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion/handler"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"
)

func init() {
	// Initialize logger for tests
	logger.Log = zaptest.NewLogger(nil).Named("test")
}

// Sample test data
var (
	testTenantID = "tenant-1"
	testChatID   = "chat-1"
	testMsgID    = "msg-123"
)

// Utility function to create test context and metadata
func setupTest(t *testing.T) (context.Context, *model.MessageMetadata) {
	ctx := context.WithValue(context.Background(), "test", t.Name())
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	metadata := &model.MessageMetadata{
		MessageID:        testMsgID,
		MessageSubject:   "v1.historical.chats",
		CompanyID:        testTenantID,
		StreamSequence:   1,
		ConsumerSequence: 1,
	}

	return ctx, metadata
}

// TestMockHistoricalHandler demonstrates how to use the MockHistoricalHandler
func TestMockHistoricalHandler(t *testing.T) {
	// Create the mock handler
	mockHandler := new(MockHistoricalHandler)

	// Setup test data
	ctx, metadata := setupTest(t)
	eventType := model.EventType("v1.historical.chats")
	rawEvent := []byte(`{"chats":[{"id":"chat-1"}]}`)

	// Setup expectations
	mockHandler.On("HandleEvent", mock.Anything, eventType, metadata, rawEvent).Return(nil)

	// Call the mock handler
	err := mockHandler.HandleEvent(ctx, eventType, metadata, rawEvent)

	// Assert
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
}

// TestMockRealtimeHandler demonstrates how to use the MockRealtimeHandler
func TestMockRealtimeHandler(t *testing.T) {
	// Create the mock handler
	mockHandler := new(MockRealtimeHandler)

	// Setup test data
	ctx, metadata := setupTest(t)
	metadata.MessageSubject = "v1.chats.upsert"
	eventType := model.EventType(metadata.MessageSubject)
	rawEvent := []byte(`{"id":"chat-1"}`)

	// Setup expectations
	mockHandler.On("HandleEvent", mock.Anything, eventType, metadata, rawEvent).Return(nil)

	// Call the mock handler
	err := mockHandler.HandleEvent(ctx, eventType, metadata, rawEvent)

	// Assert
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
}

// TestMockHistoricalServiceWithHandler tests a real handler with a mock service
func TestMockHistoricalServiceWithHandler(t *testing.T) {
	// Create the mock service
	mockService := new(MockHistoricalService)

	// Create a real handler with the mock service
	realHandler := handler.NewHistoricalHandler(mockService)

	// Setup test data
	ctx, metadata := setupTest(t)
	eventType := model.V1HistoricalChats
	rawEvent := []byte(`{"chats":[{"id":"chat-1","cluster":"tenant-1"}]}`)

	// Expected service calls
	expectedChats := []model.UpsertChatPayload{
		{
			ChatID:    testChatID,
			CompanyID: testTenantID,
		},
	}

	// Setup expectations on the mock service
	mockService.On("ProcessHistoricalChats", mock.Anything, mock.AnythingOfType("[]model.UpsertChatPayload"), mock.AnythingOfType("*model.LastMetadata")).
		Run(func(args mock.Arguments) {
			// Validate the service receives the expected arguments
			actualChats := args.Get(1).([]model.UpsertChatPayload)
			require.Len(t, actualChats, 1)
			assert.Equal(t, expectedChats[0].ChatID, actualChats[0].ChatID)
			assert.Equal(t, expectedChats[0].CompanyID, actualChats[0].CompanyID)
		}).
		Return(nil)

	// Call the real handler
	err := realHandler.HandleEvent(ctx, eventType, metadata, rawEvent)

	// Assert
	assert.NoError(t, err)
	mockService.AssertExpectations(t)
}

// TestMockRealtimeServiceWithHandler tests a real handler with a mock service
func TestMockRealtimeServiceWithHandler(t *testing.T) {
	// Create the mock service
	mockService := new(MockRealtimeService)

	// Create a real handler with the mock service
	realHandler := handler.NewRealtimeHandler(mockService)

	// Setup test data
	ctx, metadata := setupTest(t)
	eventType := model.V1ChatsUpsert
	rawEvent := []byte(`{"id":"chat-1","cluster":"tenant-1"}`)

	// Expected service calls
	expectedChat := model.UpsertChatPayload{
		ChatID:    testChatID,
		CompanyID: testTenantID,
	}

	// Setup expectations on the mock service
	mockService.On("UpsertChat", mock.Anything, mock.AnythingOfType("model.UpsertChatPayload"), mock.AnythingOfType("*model.LastMetadata")).
		Run(func(args mock.Arguments) {
			// Validate the service receives the expected arguments
			actualChat := args.Get(1).(model.UpsertChatPayload)
			assert.Equal(t, expectedChat.ChatID, actualChat.ChatID)
			assert.Equal(t, expectedChat.CompanyID, actualChat.CompanyID)
		}).
		Return(nil)

	// Call the real handler
	err := realHandler.HandleEvent(ctx, eventType, metadata, rawEvent)

	// Assert
	assert.NoError(t, err)
	mockService.AssertExpectations(t)
}

// TestMockServiceError demonstrates error handling
func TestMockServiceError(t *testing.T) {
	// Create the mock service
	mockService := new(MockHistoricalService)

	// Create a real handler with the mock service
	realHandler := handler.NewHistoricalHandler(mockService)

	// Setup test data
	ctx, metadata := setupTest(t)
	eventType := model.V1HistoricalChats
	rawEvent := []byte(`{"chats":[{"id":"chat-1","cluster":"tenant-1"}]}`)

	// Expected error
	expectedErr := errors.New("service error")

	// Setup expectations on the mock service - return an error
	mockService.On("ProcessHistoricalChats", mock.Anything, mock.AnythingOfType("[]model.UpsertChatPayload"), mock.AnythingOfType("*model.LastMetadata")).
		Return(expectedErr)

	// Call the real handler
	err := realHandler.HandleEvent(ctx, eventType, metadata, rawEvent)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	mockService.AssertExpectations(t)
}
