package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion/handler"
	mockhandler "gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion/handler/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"
)

// Helper function to create context and basic metadata
func setupHistoricalTest(t *testing.T) (context.Context, *model.MessageMetadata) {
	ctx := logger.WithLogger(context.Background(), zaptest.NewLogger(t))
	metadata := &model.MessageMetadata{
		MessageID:      "nats-msg-hist-1",
		MessageSubject: "", // Will be set per test case
		CompanyID:      "test-hist-company",
		Timestamp:      time.Now(),
		Stream:         "test-hist-stream",
		Consumer:       "test-hist-consumer",
	}
	return ctx, metadata
}

// --- Test HandleEvent Routing ---

func TestHistoricalHandler_HandleEvent_Routing(t *testing.T) {
	ctx, metadata := setupHistoricalTest(t)

	testCases := []struct {
		name            string
		eventType       model.EventType
		subject         string
		payload         []byte
		expectCall      string // Service method expected to be called
		expectFatal     bool
		expectRetryable bool
	}{
		{
			name:        "route historical chats",
			eventType:   model.V1HistoricalChats,
			subject:     string(model.V1HistoricalChats) + ".test-hist-company",
			payload:     []byte(`{"chats":[{"chat_id":"c1"}]}`),
			expectCall:  "ProcessHistoricalChats",
			expectFatal: false, // Assuming service call succeeds
		},
		{
			name:        "route historical messages",
			eventType:   model.V1HistoricalMessages,
			subject:     string(model.V1HistoricalMessages) + ".test-hist-company",
			payload:     []byte(`{"messages":[{"message_id":"m1"}]}`),
			expectCall:  "ProcessHistoricalMessages",
			expectFatal: false,
		},
		{
			name:        "route historical contacts",
			eventType:   model.V1HistoricalContacts,
			subject:     string(model.V1HistoricalContacts) + ".test-hist-company",
			payload:     []byte(`{"contacts":[{"id":"ct1"}]}`),
			expectCall:  "ProcessHistoricalContacts",
			expectFatal: false,
		},
		{
			name:        "unsupported event type",
			eventType:   model.EventType("v1.history.unknown"),
			subject:     "v1.history.unknown.test-hist-company",
			payload:     []byte(`{}`),
			expectCall:  "",   // No service call expected
			expectFatal: true, // Unsupported type is fatal
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset mock for each test case
			mockService := new(mockhandler.MockHistoricalService)
			h := handler.NewHistoricalHandler(mockService)
			metadata.MessageSubject = tc.subject

			// Setup expectation only if a service call is expected
			if tc.expectCall != "" {
				// Assume service call succeeds unless testing service errors
				mockService.On(tc.expectCall, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			}

			err := h.HandleEvent(ctx, tc.eventType, metadata, tc.payload)

			if tc.expectFatal || tc.expectRetryable {
				assert.Error(t, err)
				if tc.expectFatal {
					var fatalErr *apperrors.FatalError
					assert.True(t, errors.As(err, &fatalErr), "Expected FatalError for %s, got %T", tc.name, err)
				}
				if tc.expectRetryable {
					var retryableErr *apperrors.RetryableError
					assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError for %s, got %T", tc.name, err)
				}
			} else {
				assert.NoError(t, err)
			}

			// Assert that the expected primary call was made
			mockService.AssertExpectations(t)
			if tc.expectCall != "" {
				mockService.AssertCalled(t, tc.expectCall, mock.Anything, mock.Anything, mock.Anything)
			} else {
				mockService.AssertNumberOfCalls(t, "ProcessHistoricalChats", 0)
				mockService.AssertNumberOfCalls(t, "ProcessHistoricalMessages", 0)
				mockService.AssertNumberOfCalls(t, "ProcessHistoricalContacts", 0)
			}
		})
	}
}

// --- Test Individual Handlers ---

// Generic test function for success cases
func testHistoricalHandlerSuccess[P any, S any](t *testing.T, eventType model.EventType, serviceMethodName string, payload P, expectedServiceArg S) {
	mockService := new(mockhandler.MockHistoricalService)
	ctx, metadata := setupHistoricalTest(t)
	metadata.MessageSubject = string(eventType) + ".test-hist-company"

	rawPayload, err := json.Marshal(payload)
	assert.NoError(t, err)

	// Expect the primary service call
	mockService.On(serviceMethodName, ctx, expectedServiceArg, metadata.ToLastMetadata()).Return(nil)

	// For messages, also expect the non-blocking call (use Maybe)
	if eventType == model.V1HistoricalMessages {
		mockService.On("CreateOnboardingLogIfNeeded", mock.Anything, metadata.CompanyID, mock.AnythingOfType("string")).Maybe()
	}

	// Create handler instance here to call HandleEvent
	handlerInstance := handler.NewHistoricalHandler(mockService)
	err = handlerInstance.HandleEvent(ctx, eventType, metadata, rawPayload)

	assert.NoError(t, err)
	mockService.AssertExpectations(t)
	mockService.AssertCalled(t, serviceMethodName, ctx, expectedServiceArg, metadata.ToLastMetadata())
}

// Generic test function for unmarshal errors
func testHistoricalHandlerUnmarshalError(t *testing.T, eventType model.EventType, serviceMethodName string) {
	mockService := new(mockhandler.MockHistoricalService)
	ctx, metadata := setupHistoricalTest(t)
	metadata.MessageSubject = string(eventType) + ".test-hist-company"

	rawPayload := []byte(`invalid json`)

	// Create handler instance here to call HandleEvent
	handlerInstance := handler.NewHistoricalHandler(mockService)
	err := handlerInstance.HandleEvent(ctx, eventType, metadata, rawPayload)

	assert.Error(t, err)
	// Check for FatalError
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError for unmarshal error, got %T", err)
	assert.Contains(t, err.Error(), "failed to unmarshal")
	mockService.AssertNotCalled(t, serviceMethodName, mock.Anything, mock.Anything, mock.Anything)
}

// Generic test function for service errors
func testHistoricalHandlerServiceError[P any, S any](t *testing.T, eventType model.EventType, serviceMethodName string, payload P, expectedServiceArg S) {
	ctx, metadata := setupHistoricalTest(t)
	metadata.MessageSubject = string(eventType) + ".test-hist-company"

	rawPayload, err := json.Marshal(payload)
	assert.NoError(t, err)

	// Simulate different types of errors from the service
	testErrCases := []struct {
		name            string
		serviceErr      error
		expectFatal     bool
		expectRetryable bool
	}{
		{
			name:            "Service Fatal Error",
			serviceErr:      apperrors.NewFatal(errors.New("service fatal"), "service fatal error"),
			expectFatal:     true,
			expectRetryable: false,
		},
		{
			name:            "Service Retryable Error",
			serviceErr:      apperrors.NewRetryable(errors.New("service retryable"), "service retryable error"),
			expectFatal:     false,
			expectRetryable: true,
		},
	}

	for _, tec := range testErrCases {
		t.Run(tec.name, func(t *testing.T) {
			// Reset mock calls for sub-test
			mockService := new(mockhandler.MockHistoricalService)
			h := handler.NewHistoricalHandler(mockService)

			mockService.On(serviceMethodName, ctx, expectedServiceArg, metadata.ToLastMetadata()).Return(tec.serviceErr).Once()

			returnedErr := h.HandleEvent(ctx, eventType, metadata, rawPayload)

			assert.Error(t, returnedErr)
			assert.Equal(t, tec.serviceErr, returnedErr) // Handler should return the service error directly

			if tec.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(returnedErr, &fatalErr), "Expected FatalError, got %T", returnedErr)
			}
			if tec.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(returnedErr, &retryableErr), "Expected RetryableError, got %T", returnedErr)
			}

			mockService.AssertExpectations(t)
			mockService.AssertCalled(t, serviceMethodName, ctx, expectedServiceArg, metadata.ToLastMetadata())
		})
	}
}

// --- Chats Tests ---
func TestHistoricalHandler_Chats(t *testing.T) {
	payload := model.HistoryChatPayload{Chats: []model.UpsertChatPayload{{ChatID: "c1", Jid: "jid1"}}}
	expectedArg := payload.Chats
	testHistoricalHandlerSuccess(t, model.V1HistoricalChats, "ProcessHistoricalChats", payload, expectedArg)
}
func TestHistoricalHandler_Chats_Empty(t *testing.T) {
	mockService := new(mockhandler.MockHistoricalService)
	ctx, metadata := setupHistoricalTest(t)
	metadata.MessageSubject = string(model.V1HistoricalChats) + ".test-hist-company"
	rawPayload := []byte(`{"chats":[]}`)

	// Create handler instance here to call HandleEvent
	handlerInstance := handler.NewHistoricalHandler(mockService)
	err := handlerInstance.HandleEvent(ctx, model.V1HistoricalChats, metadata, rawPayload)

	assert.NoError(t, err)
	mockService.AssertNotCalled(t, "ProcessHistoricalChats", mock.Anything, mock.Anything, mock.Anything)
}
func TestHistoricalHandler_Chats_UnmarshalError(t *testing.T) {
	testHistoricalHandlerUnmarshalError(t, model.V1HistoricalChats, "ProcessHistoricalChats")
}
func TestHistoricalHandler_Chats_ServiceError(t *testing.T) {
	payload := model.HistoryChatPayload{Chats: []model.UpsertChatPayload{{ChatID: "c1", Jid: "jid1"}}}
	expectedArg := payload.Chats
	testHistoricalHandlerServiceError(t, model.V1HistoricalChats, "ProcessHistoricalChats", payload, expectedArg)
}

// --- Messages Tests ---
func TestHistoricalHandler_Messages(t *testing.T) {
	payload := model.HistoryMessagePayload{Messages: []model.UpsertMessagePayload{{MessageID: "m1", Jid: "jid1"}}}
	expectedArg := payload.Messages
	testHistoricalHandlerSuccess(t, model.V1HistoricalMessages, "ProcessHistoricalMessages", payload, expectedArg)
}
func TestHistoricalHandler_Messages_Empty(t *testing.T) {
	mockService := new(mockhandler.MockHistoricalService)
	ctx, metadata := setupHistoricalTest(t)
	metadata.MessageSubject = string(model.V1HistoricalMessages) + ".test-hist-company"
	rawPayload := []byte(`{"messages":[]}`)

	// Create handler instance here to call HandleEvent
	handlerInstance := handler.NewHistoricalHandler(mockService)
	err := handlerInstance.HandleEvent(ctx, model.V1HistoricalMessages, metadata, rawPayload)

	assert.NoError(t, err)
	mockService.AssertNotCalled(t, "ProcessHistoricalMessages", mock.Anything, mock.Anything, mock.Anything)
	mockService.AssertNotCalled(t, "CreateOnboardingLogIfNeeded", mock.Anything, mock.Anything, mock.Anything)
}
func TestHistoricalHandler_Messages_UnmarshalError(t *testing.T) {
	testHistoricalHandlerUnmarshalError(t, model.V1HistoricalMessages, "ProcessHistoricalMessages")
}
func TestHistoricalHandler_Messages_ServiceError(t *testing.T) {
	payload := model.HistoryMessagePayload{Messages: []model.UpsertMessagePayload{{MessageID: "m1", Jid: "jid1"}}}
	expectedArg := payload.Messages
	testHistoricalHandlerServiceError(t, model.V1HistoricalMessages, "ProcessHistoricalMessages", payload, expectedArg)
}

// --- Contacts Tests ---
func TestHistoricalHandler_Contacts(t *testing.T) {
	payload := model.HistoryContactPayload{Contacts: []model.UpsertContactPayload{{PhoneNumber: "628888888845"}}}
	expectedArg := payload.Contacts
	testHistoricalHandlerSuccess(t, model.V1HistoricalContacts, "ProcessHistoricalContacts", payload, expectedArg)
}
func TestHistoricalHandler_Contacts_Empty(t *testing.T) {
	mockService := new(mockhandler.MockHistoricalService)
	ctx, metadata := setupHistoricalTest(t)
	metadata.MessageSubject = string(model.V1HistoricalContacts) + ".test-hist-company"
	rawPayload := []byte(`{"contacts":[]}`)

	// Create handler instance here to call HandleEvent
	handlerInstance := handler.NewHistoricalHandler(mockService)
	err := handlerInstance.HandleEvent(ctx, model.V1HistoricalContacts, metadata, rawPayload)

	assert.NoError(t, err)
	mockService.AssertNotCalled(t, "ProcessHistoricalContacts", mock.Anything, mock.Anything, mock.Anything)
}
func TestHistoricalHandler_Contacts_UnmarshalError(t *testing.T) {
	testHistoricalHandlerUnmarshalError(t, model.V1HistoricalContacts, "ProcessHistoricalContacts")
}
func TestHistoricalHandler_Contacts_ServiceError(t *testing.T) {
	payload := model.HistoryContactPayload{Contacts: []model.UpsertContactPayload{{PhoneNumber: "628888888845"}}}
	expectedArg := payload.Contacts
	testHistoricalHandlerServiceError(t, model.V1HistoricalContacts, "ProcessHistoricalContacts", payload, expectedArg)
}
