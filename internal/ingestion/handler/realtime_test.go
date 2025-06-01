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
func setupRealtimeTest(t *testing.T) (context.Context, *model.MessageMetadata) {
	ctx := logger.WithLogger(context.Background(), zaptest.NewLogger(t))
	metadata := &model.MessageMetadata{
		MessageID:      "nats-msg-1",
		MessageSubject: "", // Will be set per test case
		CompanyID:      "test-company",
		Timestamp:      time.Now(),
		Stream:         "test-stream",
		Consumer:       "test-consumer",
	}
	return ctx, metadata
}

// --- Test HandleEvent Routing ---

func TestRealtimeHandler_HandleEvent_Routing(t *testing.T) {
	ctx, metadata := setupRealtimeTest(t)

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
			name:        "route chat upsert",
			eventType:   model.V1ChatsUpsert,
			subject:     string(model.V1ChatsUpsert) + ".test-company",
			payload:     []byte(`{"chat_id":"chat1"}`),
			expectCall:  "UpsertChat",
			expectFatal: false, // Assuming service call succeeds
		},
		{
			name:        "route chat update",
			eventType:   model.V1ChatsUpdate,
			subject:     string(model.V1ChatsUpdate) + ".test-company",
			payload:     []byte(`{"id":"chat1"}`),
			expectCall:  "UpdateChat",
			expectFatal: false,
		},
		{
			name:        "route message upsert",
			eventType:   model.V1MessagesUpsert,
			subject:     string(model.V1MessagesUpsert) + ".test-company",
			payload:     []byte(`{"message_id":"msg1"}`),
			expectCall:  "UpsertMessage",
			expectFatal: false,
		},
		{
			name:        "route message update",
			eventType:   model.V1MessagesUpdate,
			subject:     string(model.V1MessagesUpdate) + ".test-company",
			payload:     []byte(`{"id":"msg1"}`),
			expectCall:  "UpdateMessage",
			expectFatal: false,
		},
		{
			name:        "route contact upsert",
			eventType:   model.V1ContactsUpsert,
			subject:     string(model.V1ContactsUpsert) + ".test-company",
			payload:     []byte(`{"id":"contact1"}`),
			expectCall:  "UpsertContact",
			expectFatal: false,
		},
		{
			name:        "route contact update",
			eventType:   model.V1ContactsUpdate,
			subject:     string(model.V1ContactsUpdate) + ".test-company",
			payload:     []byte(`{"id":"contact1"}`),
			expectCall:  "UpdateContact",
			expectFatal: false,
		},
		{
			name:        "route agent event",
			eventType:   model.V1Agents,
			subject:     string(model.V1Agents) + ".agent123.test-company", // Subject needs agent ID
			payload:     []byte(`{"agent_id":"agent123"}`),
			expectCall:  "UpsertAgent",
			expectFatal: false,
		},
		{
			name:        "unsupported event type",
			eventType:   model.EventType("v1.unknown.event"),
			subject:     "v1.unknown.event.test-company",
			payload:     []byte(`{}`),
			expectCall:  "", // No service call expected
			expectFatal: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset mock for each test case
			mockService := new(mockhandler.MockRealtimeService)
			h := handler.NewRealtimeHandler(mockService)
			metadata.MessageSubject = tc.subject

			// Setup expectation only if a service call is expected
			if tc.expectCall != "" {
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

			// Assert that the expected call was made (or not made if expectCall is empty)
			mockService.AssertExpectations(t)
			if tc.expectCall != "" {
				mockService.AssertCalled(t, tc.expectCall, mock.Anything, mock.Anything, mock.Anything)
			} else {
				mockService.AssertNumberOfCalls(t, "UpsertChat", 0)
				mockService.AssertNumberOfCalls(t, "UpdateChat", 0)
				mockService.AssertNumberOfCalls(t, "UpsertMessage", 0)
				mockService.AssertNumberOfCalls(t, "UpdateMessage", 0)
				mockService.AssertNumberOfCalls(t, "UpsertContact", 0)
				mockService.AssertNumberOfCalls(t, "UpdateContact", 0)
				mockService.AssertNumberOfCalls(t, "UpsertAgent", 0)
			}
		})
	}
}

// --- Test Individual Handlers ---

// Generic test function for success cases using HandleEvent
func testRealtimeHandlerSuccessViaEvent[P any](t *testing.T, eventType model.EventType, serviceMethodName string, payload P, expectedEnrichedPayload P) {
	mockService := new(mockhandler.MockRealtimeService)
	ctx, metadata := setupRealtimeTest(t)
	metadata.MessageSubject = string(eventType) + ".test-company" // Ensure subject matches

	rawPayload, err := json.Marshal(payload) // Marshal original payload
	assert.NoError(t, err)

	// Expect the service call with the enriched payload provided by the caller
	mockService.On(serviceMethodName, ctx, expectedEnrichedPayload, metadata.ToLastMetadata()).Return(nil)

	// Create handler instance here to call HandleEvent
	handlerInstance := handler.NewRealtimeHandler(mockService)
	err = handlerInstance.HandleEvent(ctx, eventType, metadata, rawPayload)

	assert.NoError(t, err)
	mockService.AssertExpectations(t)
	mockService.AssertCalled(t, serviceMethodName, ctx, expectedEnrichedPayload, metadata.ToLastMetadata())
}

// Generic test function for unmarshal errors using HandleEvent
func testRealtimeHandlerUnmarshalErrorViaEvent(t *testing.T, eventType model.EventType, serviceMethodName string) {
	mockService := new(mockhandler.MockRealtimeService)
	ctx, metadata := setupRealtimeTest(t)
	metadata.MessageSubject = string(eventType) + ".test-company"

	rawPayload := []byte(`invalid json`)

	// Create handler instance here to call HandleEvent
	handlerInstance := handler.NewRealtimeHandler(mockService)
	err := handlerInstance.HandleEvent(ctx, eventType, metadata, rawPayload)

	assert.Error(t, err)
	// Check for FatalError
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError for unmarshal error, got %T", err)

	// Split assertion: check for generic part and specific part
	expectedSubstringPart1 := "failed to unmarshal"
	expectedSubstringPart2 := "payload" // Default

	if eventType == model.V1ChatsUpsert {
		expectedSubstringPart2 = "chat upsert payload"
	} else if eventType == model.V1ChatsUpdate {
		expectedSubstringPart2 = "chat update payload"
	} else if eventType == model.V1MessagesUpsert {
		expectedSubstringPart2 = "message upsert payload"
	} else if eventType == model.V1MessagesUpdate {
		expectedSubstringPart2 = "message update payload"
	} else if eventType == model.V1ContactsUpsert {
		expectedSubstringPart2 = "contact upsert payload"
	} else if eventType == model.V1ContactsUpdate {
		expectedSubstringPart2 = "contact update payload"
	} else if eventType == model.V1Agents {
		expectedSubstringPart2 = "agent payload"
	}
	// Check that the error message contains both parts
	assert.Contains(t, err.Error(), expectedSubstringPart1, "Error message should contain '%s'", expectedSubstringPart1)
	assert.Contains(t, err.Error(), expectedSubstringPart2, "Error message should contain '%s'", expectedSubstringPart2)

	mockService.AssertNotCalled(t, serviceMethodName, mock.Anything, mock.Anything, mock.Anything)
}

// Generic test function for service errors using HandleEvent
func testRealtimeHandlerServiceErrorViaEvent[P any](t *testing.T, eventType model.EventType, serviceMethodName string, payload P, expectedEnrichedPayload P) {
	ctx, metadata := setupRealtimeTest(t)
	metadata.MessageSubject = string(eventType) + ".test-company"

	rawPayload, err := json.Marshal(payload) // Marshal original payload
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
			mockService := new(mockhandler.MockRealtimeService)
			h := handler.NewRealtimeHandler(mockService)

			mockService.On(serviceMethodName, ctx, expectedEnrichedPayload, metadata.ToLastMetadata()).Return(tec.serviceErr).Once()

			returnedErr := h.HandleEvent(ctx, eventType, metadata, rawPayload)

			assert.Error(t, returnedErr)
			assert.Equal(t, tec.serviceErr, returnedErr) // Handler returns service error directly

			if tec.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(returnedErr, &fatalErr), "Expected FatalError, got %T", returnedErr)
			}
			if tec.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(returnedErr, &retryableErr), "Expected RetryableError, got %T", returnedErr)
			}

			mockService.AssertExpectations(t)
			mockService.AssertCalled(t, serviceMethodName, ctx, expectedEnrichedPayload, metadata.ToLastMetadata())
		})
	}
}

// --- Chat Tests ---
func TestRealtimeHandler_ChatUpsert(t *testing.T) {
	payload := model.UpsertChatPayload{ChatID: "c1"} // Original
	expected := payload
	expected.CompanyID = "test-company" // Enriched
	testRealtimeHandlerSuccessViaEvent(t, model.V1ChatsUpsert, "UpsertChat", payload, expected)
}
func TestRealtimeHandler_ChatUpsert_UnmarshalError(t *testing.T) {
	testRealtimeHandlerUnmarshalErrorViaEvent(t, model.V1ChatsUpsert, "UpsertChat")
}
func TestRealtimeHandler_ChatUpsert_ServiceError(t *testing.T) {
	payload := model.UpsertChatPayload{ChatID: "c1"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerServiceErrorViaEvent(t, model.V1ChatsUpsert, "UpsertChat", payload, expected)
}

func TestRealtimeHandler_ChatUpdate(t *testing.T) {
	payload := model.UpdateChatPayload{ChatID: "c1"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerSuccessViaEvent(t, model.V1ChatsUpdate, "UpdateChat", payload, expected)
}
func TestRealtimeHandler_ChatUpdate_UnmarshalError(t *testing.T) {
	testRealtimeHandlerUnmarshalErrorViaEvent(t, model.V1ChatsUpdate, "UpdateChat")
}
func TestRealtimeHandler_ChatUpdate_ServiceError(t *testing.T) {
	payload := model.UpdateChatPayload{ChatID: "c1"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerServiceErrorViaEvent(t, model.V1ChatsUpdate, "UpdateChat", payload, expected)
}
func TestRealtimeHandler_ChatUpdate_MissingID(t *testing.T) {
	mockService := new(mockhandler.MockRealtimeService)
	h := handler.NewRealtimeHandler(mockService)
	ctx, metadata := setupRealtimeTest(t)
	metadata.MessageSubject = string(model.V1ChatsUpdate) + ".test-company"
	rawPayload := []byte(`{"company_id":"test-company"}`) // Missing ID

	err := h.HandleEvent(ctx, model.V1ChatsUpdate, metadata, rawPayload)

	assert.Error(t, err)
	// Check for FatalError
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError for missing ID, got %T", err)
	assert.Contains(t, err.Error(), "chat ID is required for update")
	mockService.AssertNotCalled(t, "UpdateChat", mock.Anything, mock.Anything, mock.Anything)
}

// --- Message Tests ---
func TestRealtimeHandler_MessageUpsert(t *testing.T) {
	payload := model.UpsertMessagePayload{MessageID: "m1"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerSuccessViaEvent(t, model.V1MessagesUpsert, "UpsertMessage", payload, expected)
}
func TestRealtimeHandler_MessageUpsert_UnmarshalError(t *testing.T) {
	testRealtimeHandlerUnmarshalErrorViaEvent(t, model.V1MessagesUpsert, "UpsertMessage")
}
func TestRealtimeHandler_MessageUpsert_ServiceError(t *testing.T) {
	payload := model.UpsertMessagePayload{MessageID: "m1"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerServiceErrorViaEvent(t, model.V1MessagesUpsert, "UpsertMessage", payload, expected)
}

func TestRealtimeHandler_MessageUpdate(t *testing.T) {
	payload := model.UpdateMessagePayload{MessageID: "m1"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerSuccessViaEvent(t, model.V1MessagesUpdate, "UpdateMessage", payload, expected)
}
func TestRealtimeHandler_MessageUpdate_UnmarshalError(t *testing.T) {
	testRealtimeHandlerUnmarshalErrorViaEvent(t, model.V1MessagesUpdate, "UpdateMessage")
}
func TestRealtimeHandler_MessageUpdate_ServiceError(t *testing.T) {
	payload := model.UpdateMessagePayload{MessageID: "m1"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerServiceErrorViaEvent(t, model.V1MessagesUpdate, "UpdateMessage", payload, expected)
}
func TestRealtimeHandler_MessageUpdate_MissingID(t *testing.T) {
	mockService := new(mockhandler.MockRealtimeService)
	h := handler.NewRealtimeHandler(mockService)
	ctx, metadata := setupRealtimeTest(t)
	metadata.MessageSubject = string(model.V1MessagesUpdate) + ".test-company"
	rawPayload := []byte(`{"status":"read"}`) // Missing ID

	err := h.HandleEvent(ctx, model.V1MessagesUpdate, metadata, rawPayload)

	assert.Error(t, err)
	// Check for FatalError
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError for missing ID, got %T", err)
	assert.Contains(t, err.Error(), "message ID is required for update")
	mockService.AssertNotCalled(t, "UpdateMessage", mock.Anything, mock.Anything, mock.Anything)
}

// --- Contact Tests ---
func TestRealtimeHandler_ContactUpsert(t *testing.T) {
	payload := model.UpsertContactPayload{PhoneNumber: "628888888845"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerSuccessViaEvent(t, model.V1ContactsUpsert, "UpsertContact", payload, expected)
}
func TestRealtimeHandler_ContactUpsert_UnmarshalError(t *testing.T) {
	testRealtimeHandlerUnmarshalErrorViaEvent(t, model.V1ContactsUpsert, "UpsertContact")
}
func TestRealtimeHandler_ContactUpsert_ServiceError(t *testing.T) {
	payload := model.UpsertContactPayload{PhoneNumber: "628888888845"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerServiceErrorViaEvent(t, model.V1ContactsUpsert, "UpsertContact", payload, expected)
}

func TestRealtimeHandler_ContactUpdate(t *testing.T) {
	payload := model.UpdateContactPayload{PhoneNumber: "628888888845"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerSuccessViaEvent(t, model.V1ContactsUpdate, "UpdateContact", payload, expected)
}
func TestRealtimeHandler_ContactUpdate_UnmarshalError(t *testing.T) {
	testRealtimeHandlerUnmarshalErrorViaEvent(t, model.V1ContactsUpdate, "UpdateContact")
}
func TestRealtimeHandler_ContactUpdate_ServiceError(t *testing.T) {
	payload := model.UpdateContactPayload{PhoneNumber: "628888888845"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerServiceErrorViaEvent(t, model.V1ContactsUpdate, "UpdateContact", payload, expected)
}

// --- Agent Tests ---
func TestRealtimeHandler_AgentEvent(t *testing.T) {
	payload := model.UpsertAgentPayload{AgentID: "agent-xyz"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerSuccessViaEvent(t, model.V1Agents, "UpsertAgent", payload, expected)
}
func TestRealtimeHandler_AgentEvent_UnmarshalError(t *testing.T) {
	testRealtimeHandlerUnmarshalErrorViaEvent(t, model.V1Agents, "UpsertAgent")
}
func TestRealtimeHandler_AgentEvent_ServiceError(t *testing.T) {
	payload := model.UpsertAgentPayload{AgentID: "agent-xyz"}
	expected := payload
	expected.CompanyID = "test-company"
	testRealtimeHandlerServiceErrorViaEvent(t, model.V1Agents, "UpsertAgent", payload, expected)
}
