package ingestion

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"
)

// MockHandler definition is now in jetstream_test.go

func TestRouter_Register(t *testing.T) {
	// Create a new router and mock handler
	router := NewRouter()
	mockHandler := new(MockHandler)

	// Create a handler function that forwards to the mock
	handler := func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		return mockHandler.Handle(ctx, eventType, metadata, rawEvent)
	}

	// Register the handler
	eventType := model.EventType("test.event")
	router.Register(eventType, handler)

	// Verify the handler was registered by checking the map directly
	assert.NotNil(t, router.handlers[eventType], "Handler should be registered")
}

func TestRouter_RegisterDefault(t *testing.T) {
	// Create a new router and mock handler
	router := NewRouter()
	mockHandler := new(MockHandler)

	// Create a handler function that forwards to the mock
	handler := func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		return mockHandler.Handle(ctx, eventType, metadata, rawEvent)
	}

	// Register the default handler
	router.RegisterDefault(handler)

	// Verify the default handler was registered
	assert.NotNil(t, router.defaultHandler, "Default handler should be registered")
}

func TestRouter_Route_ExactMatch(t *testing.T) {
	// Create a new router and mock handler
	router := NewRouter()
	mockHandler := new(MockHandler)

	// Create a handler function that forwards to the mock
	handler := func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		return mockHandler.Handle(ctx, eventType, metadata, rawEvent)
	}

	// Register the handler for a specific event type using a constant
	eventType := model.V1MessagesUpsert // Use a known constant
	router.Register(eventType, handler)

	// Create message metadata and raw event
	rawEvent := []byte(`{"key":"value"}`)
	metadata := &model.MessageMetadata{
		MessageSubject: string(eventType), // Use the constant string
		MessageID:      "msg-123",
		CompanyID:      "tenant-1",
	}

	// Set up expectations for the mock handler
	mockHandler.On("Handle", mock.Anything, eventType, metadata, rawEvent).Return(nil)

	// Create test context with logger
	ctx := logger.WithLogger(context.Background(), zaptest.NewLogger(t))

	// Route the event
	err := router.Route(ctx, metadata, rawEvent)

	// Verify expectations
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
}

func TestRouter_Route_DefaultHandler(t *testing.T) {
	// Create a new router and mock handlers
	router := NewRouter()
	mockDefaultHandler := new(MockHandler)

	// Create handler functions that forward to the mocks
	defaultHandler := func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		return mockDefaultHandler.Handle(ctx, eventType, metadata, rawEvent)
	}

	// Register only the default handler
	router.RegisterDefault(defaultHandler)

	// Create message metadata and raw event for an UNREGISTERED event type
	// MapToBaseEventType needs to fail for this test to hit the default handler.
	// Use a subject that MapToBaseEventType won't recognize.
	unregisteredSubject := "invalid.subject.format"
	rawEvent := []byte(`{"key":"value"}`)
	metadata := &model.MessageMetadata{
		MessageSubject: unregisteredSubject,
		MessageID:      "msg-456",
		CompanyID:      "tenant-2",
	}

	// Set up expectations for the default handler.
	// The eventType passed to the default handler will be derived by MapToBaseEventType,
	// which will be empty if the subject is invalid.
	mockDefaultHandler.On("Handle", mock.Anything, model.EventType(""), metadata, rawEvent).Return(nil)

	// Create test context with logger
	ctx := logger.WithLogger(context.Background(), zaptest.NewLogger(t))

	// Route the event
	err := router.Route(ctx, metadata, rawEvent)

	// Verify expectations
	assert.NoError(t, err)
	mockDefaultHandler.AssertExpectations(t)
}

func TestRouter_Route_NoHandler(t *testing.T) {
	// Create a new router without any handlers
	router := NewRouter()

	// Create message metadata and raw event for an UNREGISTERED type
	unregisteredSubject := "another.invalid.subject"
	rawEvent := []byte(`{"key":"value"}`)
	metadata := &model.MessageMetadata{
		MessageSubject: unregisteredSubject,
		MessageID:      "msg-789",
		CompanyID:      "tenant-3",
	}

	// Create test context with logger
	ctx := logger.WithLogger(context.Background(), zaptest.NewLogger(t))

	// Route the event
	err := router.Route(ctx, metadata, rawEvent)

	// Verify no error is returned, and no handler was called (implicit check)
	assert.NoError(t, err)
}

func TestRouter_Route_HandleError(t *testing.T) {
	// Create a new router and mock handler
	router := NewRouter()
	mockHandler := new(MockHandler)

	// Create a handler function that forwards to the mock
	handler := func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		return mockHandler.Handle(ctx, eventType, metadata, rawEvent)
	}

	// Register the handler for a specific event type using a constant
	eventType := model.V1MessagesUpdate // Use a known constant
	router.Register(eventType, handler)

	// Create message metadata and raw event
	rawEvent := []byte(`{"key":"value"}`)
	metadata := &model.MessageMetadata{
		MessageSubject: string(eventType), // Use the constant string
		MessageID:      "msg-123",
		CompanyID:      "tenant-1",
	}

	// Set up expectations for the mock handler to return an error
	expectedErr := errors.New("handler error")
	mockHandler.On("Handle", mock.Anything, eventType, metadata, rawEvent).Return(expectedErr)

	// Create test context with logger
	ctx := logger.WithLogger(context.Background(), zaptest.NewLogger(t))

	// Route the event
	err := router.Route(ctx, metadata, rawEvent)

	// Verify the error is propagated
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	mockHandler.AssertExpectations(t)
}

func TestRouter_Route_TenantContext(t *testing.T) {
	// Create a new router and mock handler
	router := NewRouter()
	mockHandler := new(MockHandler)

	// Create a handler function that checks for tenant ID in context
	handler := func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		// Extract tenant ID from context
		companyID, err := tenant.FromContext(ctx)
		if err != nil {
			return err
		}

		// Verify tenant ID matches metadata
		if companyID != metadata.CompanyID {
			return errors.New("tenant ID mismatch")
		}

		return mockHandler.Handle(ctx, eventType, metadata, rawEvent)
	}

	// Register the handler for a specific event type using a constant
	eventType := model.V1ContactsUpsert // Use a known constant
	router.Register(eventType, handler)

	// Create message metadata and raw event
	rawEvent := []byte(`{"key":"value"}`)
	metadata := &model.MessageMetadata{
		MessageSubject: string(eventType), // Use the constant string
		MessageID:      "msg-123",
		CompanyID:      "tenant-1",
	}

	// Set up expectations for the mock handler
	mockHandler.On("Handle", mock.Anything, eventType, metadata, rawEvent).Return(nil)

	// Create test context with logger
	ctx := logger.WithLogger(context.Background(), zaptest.NewLogger(t))

	// Route the event
	err := router.Route(ctx, metadata, rawEvent)

	// Verify the tenant ID was correctly set in context and the handler succeeded
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
}

func TestRouter_Route_LoggerContext(t *testing.T) {
	// Create a new router and mock handler
	router := NewRouter()
	mockHandler := new(MockHandler)

	// Create a handler function that checks for logger in context
	handler := func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		// Extract logger from context
		log := logger.FromContext(ctx)

		// Verify logger exists and has fields
		if log == nil {
			return errors.New("logger not found in context")
		}

		// Use a logger observer to verify fields
		// In a real test, we'd use zaptest/observer, but this example just checks for non-nil

		return mockHandler.Handle(ctx, eventType, metadata, rawEvent)
	}

	// Register the handler for a specific event type using a constant
	eventType := model.V1ContactsUpdate // Use a known constant
	router.Register(eventType, handler)

	// Create message metadata and raw event
	rawEvent := []byte(`{"key":"value"}`)
	metadata := &model.MessageMetadata{
		MessageSubject: string(eventType), // Use the constant string
		MessageID:      "msg-123",
		CompanyID:      "tenant-1",
	}

	// Set up expectations for the mock handler
	mockHandler.On("Handle", mock.Anything, eventType, metadata, rawEvent).Return(nil)

	// Create test context with logger
	baseLogger := zaptest.NewLogger(t)
	ctx := logger.WithLogger(context.Background(), baseLogger)

	// Route the event
	err := router.Route(ctx, metadata, rawEvent)

	// Verify the logger was correctly enhanced in context and the handler succeeded
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
}

func TestRouter_Route_VersionParsing(t *testing.T) {
	// Create a new router and mock handler
	router := NewRouter()
	mockHandler := new(MockHandler)

	// Create a handler function that verifies event type version
	handler := func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
		// Check if version is correctly parsed by the router's call to MapToBaseEventType
		version := eventType.GetVersion()
		if version != "v1" { // Assuming V1Agents is a v1 type
			return errors.New("incorrect version parsing")
		}

		return mockHandler.Handle(ctx, eventType, metadata, rawEvent)
	}

	// Register the handler for a specific versioned event type using a constant
	// The router internally uses MapToBaseEventType, so we register with the base type
	// but the handler will receive the full type derived from the subject.
	eventTypeConstant := model.V1Agents // Use a known v1 constant
	router.Register(eventTypeConstant, handler)

	// Create message metadata and raw event using the constant string as subject
	rawEvent := []byte(`{"key":"value"}`)
	metadata := &model.MessageMetadata{
		MessageSubject: string(eventTypeConstant),
		MessageID:      "msg-123",
		CompanyID:      "tenant-1",
	}

	// Set up expectations for the mock handler, expecting the full constant type
	mockHandler.On("Handle", mock.Anything, eventTypeConstant, metadata, rawEvent).Return(nil)

	// Create test context with logger
	ctx := logger.WithLogger(context.Background(), zaptest.NewLogger(t))

	// Route the event
	err := router.Route(ctx, metadata, rawEvent)

	// Verify the version was correctly parsed and the handler succeeded
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
}
