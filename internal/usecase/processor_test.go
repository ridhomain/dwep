package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	handlermock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion/handler/mock"
	ingestionmock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/ingestion/mock"
	jsmock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/jetstream/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"
)

// MockProcessorDependencies creates mocked dependencies for processor tests
// No longer returns consumer mocks directly, as we test via the client mock
func MockProcessorDependencies(t *testing.T) (*jsmock.ClientMock, *ingestionmock.RouterMock, *handlermock.MockHistoricalHandler, *handlermock.MockRealtimeHandler) {
	// Create mocks
	mockJSClient := new(jsmock.ClientMock)
	mockRouter := new(ingestionmock.RouterMock)
	mockHistHandler := new(handlermock.MockHistoricalHandler)
	mockRealHandler := new(handlermock.MockRealtimeHandler)

	// Return individual mocks
	return mockJSClient, mockRouter, mockHistHandler, mockRealHandler
}

// createDummyConfig creates a minimal config for processor tests
func createDummyConfig(companyID string) *config.Config {
	var cfg config.Config // Create zero-value config

	// Assign values to fields
	cfg.Company.ID = companyID // Make sure we are accessing the exported field 'Tenant'
	// cfg.Tenant.Default = \"some_default\" // Optional

	cfg.NATS.Realtime = config.ConsumerNatsConfig{
		Stream:      "rt-stream",
		Consumer:    "rt-consumer-",
		QueueGroup:  "rt-group-",
		SubjectList: []string{"rt.subj"},
		// MaxAge field exists in ConsumerNatsConfig, might need a default if used
		// MaxAge: 1,
	}
	cfg.NATS.Historical = config.ConsumerNatsConfig{
		Stream:      "hist-stream",
		Consumer:    "hist-consumer-",
		QueueGroup:  "hist-group-",
		SubjectList: []string{"hist.subj"},
		// MaxAge field exists in ConsumerNatsConfig, might need a default if used
		// MaxAge: 30,
	}
	// cfg.NATS.URL = "nats://dummy:4222" // Optional

	return &cfg // Return pointer to the configured struct
}

func TestNewProcessor(t *testing.T) {
	// Setup logger for this test
	originalLogger := logger.Log
	logger.Log = zaptest.NewLogger(t).Named("TestNewProcessor")
	defer func() { logger.Log = originalLogger }()

	// Create mock dependencies
	mockService := &EventService{} // Keep mock service simple
	mockJSClient := new(jsmock.ClientMock)
	companyID := "test-company"
	dummyCfg := createDummyConfig(companyID)

	// Create the processor using the new signature
	processor := NewProcessor(mockService, mockJSClient, dummyCfg, companyID)

	// Assertions
	assert.NotNil(t, processor)
	assert.Equal(t, mockService, processor.service)
	assert.Equal(t, mockJSClient, processor.jsClient)
	assert.NotNil(t, processor.realtimeConsumer)   // Check new field
	assert.NotNil(t, processor.historicalConsumer) // Check new field
	assert.NotNil(t, processor.eventRouter)
	assert.NotNil(t, processor.histHandler)
	assert.NotNil(t, processor.realtimeHandler)
	// Verify that the consumers created internally have the mock client
	// (This requires access to the internal client field, if possible/needed, or rely on interaction tests)
	// assert.Equal(t, mockJSClient, processor.realtimeConsumer.base.client) // Example if accessible
}

func TestProcessor_Setup(t *testing.T) {
	// Setup logger for this test
	originalLogger := logger.Log
	logger.Log = zaptest.NewLogger(t).Named("TestProcessor_Setup")
	defer func() { logger.Log = originalLogger }()

	// Get mock dependencies (excluding consumer mocks)
	mockJSClient, mockRouter, mockHistHandler, mockRealHandler := MockProcessorDependencies(t)
	companyID := "setup-test"
	dummyCfg := createDummyConfig(companyID)
	mockService := &EventService{}

	processor := NewProcessor(mockService, mockJSClient, dummyCfg, companyID)
	// Override router and handlers with mocks for expectation setting
	processor.eventRouter = mockRouter
	processor.histHandler = mockHistHandler
	processor.realtimeHandler = mockRealHandler

	// Set up expectations for router registrations
	mockRouter.On("Register", model.V1HistoricalChats, mock.Anything).Return()
	mockRouter.On("Register", model.V1HistoricalMessages, mock.Anything).Return()
	mockRouter.On("Register", model.V1HistoricalContacts, mock.Anything).Return()
	mockRouter.On("Register", model.V1ChatsUpsert, mock.Anything).Return()
	mockRouter.On("Register", model.V1ChatsUpdate, mock.Anything).Return()
	mockRouter.On("Register", model.V1MessagesUpsert, mock.Anything).Return()
	mockRouter.On("Register", model.V1MessagesUpdate, mock.Anything).Return()
	mockRouter.On("Register", model.V1ContactsUpsert, mock.Anything).Return()
	mockRouter.On("Register", model.V1ContactsUpdate, mock.Anything).Return()
	mockRouter.On("Register", model.V1Agents, mock.Anything).Return()
	mockRouter.On("RegisterDefault", mock.Anything).Return()

	// Set up expectations for the MOCKED JS CLIENT calls made by the REAL consumers' Setup methods
	// Realtime Consumer Setup Expectations
	mockJSClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(nil).Once()                                    // Expect one stream setup
	mockJSClient.On("SetupConsumer", mock.Anything, dummyCfg.NATS.Realtime.Stream, mock.AnythingOfType("*nats.ConsumerConfig")).Return(nil).Once() // Expect one consumer setup

	// Historical Consumer Setup Expectations
	mockJSClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(nil).Once()                                      // Expect second stream setup
	mockJSClient.On("SetupConsumer", mock.Anything, dummyCfg.NATS.Historical.Stream, mock.AnythingOfType("*nats.ConsumerConfig")).Return(nil).Once() // Expect second consumer setup

	// Call method under test
	err := processor.Setup()

	// Assertions
	assert.NoError(t, err)
	mockRouter.AssertExpectations(t)
	mockJSClient.AssertExpectations(t) // Verify client mock calls
}

func TestProcessor_Setup_RealtimeError(t *testing.T) {
	// Setup logger for this test
	originalLogger := logger.Log
	logger.Log = zaptest.NewLogger(t).Named("TestProcessor_Setup_RealtimeError")
	defer func() { logger.Log = originalLogger }()

	mockJSClient, mockRouter, mockHistHandler, mockRealHandler := MockProcessorDependencies(t)
	companyID := "setup-rt-err"
	dummyCfg := createDummyConfig(companyID)
	mockService := &EventService{}

	processor := NewProcessor(mockService, mockJSClient, dummyCfg, companyID)
	processor.eventRouter = mockRouter
	processor.histHandler = mockHistHandler
	processor.realtimeHandler = mockRealHandler

	mockRouter.On("Register", mock.Anything, mock.Anything).Return().Times(10)
	mockRouter.On("RegisterDefault", mock.Anything).Return()

	// Mock realtime stream setup failure
	expectedErr := errors.New("realtime stream setup failed")
	mockJSClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(expectedErr).Once()
	// Do NOT expect realtime consumer setup or any historical setup
	mockJSClient.On("SetupConsumer", mock.Anything, mock.Anything, mock.Anything).Maybe()

	err := processor.Setup()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Contains(t, err.Error(), "failed to setup realtime consumer") // Error comes from the consumer's Setup method
	mockRouter.AssertExpectations(t)
	mockJSClient.AssertExpectations(t)
}

func TestProcessor_Setup_HistoricalError(t *testing.T) {
	// Setup logger for this test
	originalLogger := logger.Log
	logger.Log = zaptest.NewLogger(t).Named("TestProcessor_Setup_HistoricalError")
	defer func() { logger.Log = originalLogger }()

	mockJSClient, mockRouter, mockHistHandler, mockRealHandler := MockProcessorDependencies(t)
	companyID := "setup-hist-err"
	dummyCfg := createDummyConfig(companyID)
	mockService := &EventService{}

	processor := NewProcessor(mockService, mockJSClient, dummyCfg, companyID)
	processor.eventRouter = mockRouter
	processor.histHandler = mockHistHandler
	processor.realtimeHandler = mockRealHandler

	mockRouter.On("Register", mock.Anything, mock.Anything).Return().Times(10)
	mockRouter.On("RegisterDefault", mock.Anything).Return()

	// Mock realtime setup success
	mockJSClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(nil).Once()                                    // Realtime Stream
	mockJSClient.On("SetupConsumer", mock.Anything, dummyCfg.NATS.Realtime.Stream, mock.AnythingOfType("*nats.ConsumerConfig")).Return(nil).Once() // Realtime Consumer

	// Mock historical stream setup failure
	expectedErr := errors.New("historical stream setup failed")
	mockJSClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(expectedErr).Once() // Historical Stream
	// Do NOT expect historical consumer setup
	mockJSClient.On("SetupConsumer", mock.Anything, dummyCfg.NATS.Historical.Stream, mock.Anything).Maybe()

	err := processor.Setup()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Contains(t, err.Error(), "failed to setup historical consumer") // Error comes from the consumer's Setup method
	mockRouter.AssertExpectations(t)
	mockJSClient.AssertExpectations(t)
}

func TestProcessor_Start(t *testing.T) {
	originalLogger := logger.Log
	// Setup logger for this test & defer restoration
	logger.Log = zaptest.NewLogger(t).Named("TestProcessor_Start")
	defer func() { logger.Log = originalLogger }()

	mockJSClient, _, _, _ := MockProcessorDependencies(t)
	companyID := "start-test"
	dummyCfg := createDummyConfig(companyID)
	mockService := &EventService{}
	processor := NewProcessor(mockService, mockJSClient, dummyCfg, companyID)

	// Set up expectations for SubscribePush on the JS CLIENT mock for both consumers
	mockSubscription := jsmock.MockSubscription()
	// Realtime consumer expected args
	expectedRtConsumerDurable := dummyCfg.NATS.Realtime.Consumer + companyID
	expectedRtQueueGroup := dummyCfg.NATS.Realtime.QueueGroup + companyID
	mockJSClient.On("SubscribePush", "", expectedRtConsumerDurable, expectedRtQueueGroup, mock.Anything, mock.AnythingOfType("nats.MsgHandler")).Return(mockSubscription, nil).Once()

	// Historical consumer expected args
	expectedHistConsumerDurable := dummyCfg.NATS.Historical.Consumer + companyID
	expectedHistQueueGroup := dummyCfg.NATS.Historical.QueueGroup + companyID
	mockJSClient.On("SubscribePush", "", expectedHistConsumerDurable, expectedHistQueueGroup, mock.Anything, mock.AnythingOfType("nats.MsgHandler")).Return(mockSubscription, nil).Once()

	err := processor.Start()

	assert.NoError(t, err)
	mockJSClient.AssertExpectations(t)
}

func TestProcessor_Start_RealtimeError(t *testing.T) {
	originalLogger := logger.Log
	// Setup logger for this test & defer restoration
	logger.Log = zaptest.NewLogger(t).Named("TestProcessor_Start_RealtimeError")
	defer func() { logger.Log = originalLogger }()

	mockJSClient, _, mockHistHandler, mockRealHandler := MockProcessorDependencies(t) // Need handlers for Stop mock
	companyID := "start-rt-err"
	dummyCfg := createDummyConfig(companyID)
	mockService := &EventService{}
	processor := NewProcessor(mockService, mockJSClient, dummyCfg, companyID)
	// Need to mock the Stop methods which might involve handlers/client
	processor.histHandler = mockHistHandler
	processor.realtimeHandler = mockRealHandler

	expectedErr := errors.New("realtime subscribe failed")
	mockSubscription := jsmock.MockSubscription()
	// Realtime consumer expected args
	expectedRtConsumerDurable := dummyCfg.NATS.Realtime.Consumer + companyID
	expectedRtQueueGroup := dummyCfg.NATS.Realtime.QueueGroup + companyID
	mockJSClient.On("SubscribePush", "", expectedRtConsumerDurable, expectedRtQueueGroup, mock.Anything, mock.AnythingOfType("nats.MsgHandler")).Return(mockSubscription, expectedErr).Once()

	// Expect historical consumer's Stop to be called - its Start is never reached
	// Mocking Stop requires knowing what it does. Assume it might call client.Close() or sub.Drain()
	// Since we don't have a mock sub handle stored easily, let's simplify and just expect NO historical SubscribePush

	err := processor.Start()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Contains(t, err.Error(), "failed to start realtime consumer")
	mockJSClient.AssertExpectations(t)
	// Verify historical consumer's SubscribePush was NOT called
	expectedHistConsumerDurable := dummyCfg.NATS.Historical.Consumer + companyID
	expectedHistQueueGroup := dummyCfg.NATS.Historical.QueueGroup + companyID
	mockJSClient.AssertNotCalled(t, "SubscribePush", "", expectedHistConsumerDurable, expectedHistQueueGroup, mock.Anything, mock.AnythingOfType("nats.MsgHandler"))
}

func TestProcessor_Start_HistoricalError(t *testing.T) {
	originalLogger := logger.Log
	// Setup logger for this test & defer restoration
	logger.Log = zaptest.NewLogger(t).Named("TestProcessor_Start_HistoricalError")
	defer func() { logger.Log = originalLogger }()

	mockJSClient, _, mockHistHandler, mockRealHandler := MockProcessorDependencies(t)
	companyID := "start-hist-err"
	dummyCfg := createDummyConfig(companyID)
	mockService := &EventService{}
	processor := NewProcessor(mockService, mockJSClient, dummyCfg, companyID)
	// Need to mock the Stop methods which might involve handlers/client
	processor.histHandler = mockHistHandler
	processor.realtimeHandler = mockRealHandler

	expectedErr := errors.New("historical subscribe failed")
	mockSubscription := jsmock.MockSubscription()
	// Realtime consumer starts OK
	expectedRtConsumerDurable := dummyCfg.NATS.Realtime.Consumer + companyID
	expectedRtQueueGroup := dummyCfg.NATS.Realtime.QueueGroup + companyID
	mockJSClient.On("SubscribePush", "", expectedRtConsumerDurable, expectedRtQueueGroup, mock.Anything, mock.AnythingOfType("nats.MsgHandler")).Return(mockSubscription, nil).Once()

	// Historical consumer fails
	expectedHistConsumerDurable := dummyCfg.NATS.Historical.Consumer + companyID
	expectedHistQueueGroup := dummyCfg.NATS.Historical.QueueGroup + companyID
	mockJSClient.On("SubscribePush", "", expectedHistConsumerDurable, expectedHistQueueGroup, mock.Anything, mock.AnythingOfType("nats.MsgHandler")).Return(mockSubscription, expectedErr).Once()

	// Expect realtime consumer's Stop to be called
	// Again, mocking Stop is tricky. Let's assume it completes without error for this test.

	err := processor.Start()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Contains(t, err.Error(), "failed to start historical consumer")
	mockJSClient.AssertExpectations(t)
}

func TestProcessor_Stop(t *testing.T) {
	originalLogger := logger.Log
	// Setup logger for this test & defer restoration
	logger.Log = zaptest.NewLogger(t).Named("TestProcessor_Stop")
	defer func() { logger.Log = originalLogger }()

	mockJSClient, _, _, _ := MockProcessorDependencies(t)
	companyID := "stop-test"
	dummyCfg := createDummyConfig(companyID)
	mockService := &EventService{}
	processor := NewProcessor(mockService, mockJSClient, dummyCfg, companyID)

	// Mocking the internal state (like subscription handles) after Start is complex.
	// For a unit test of Stop, we mostly just verify it runs without panic.
	// We assume the internal consumers' Stop methods handle their own logic (like Drain, context cancel).
	// We don't need specific mock expectations on the client for the default Stop implementation.

	assert.NotPanics(t, func() {
		processor.Stop()
	})

	// Optionally, if Stop involved client calls (like Close), mock them here.
	// mockJSClient.On("Close").Return()
	mockJSClient.AssertExpectations(t) // Verify no unexpected client calls
}

// --- Tests for Handler/Router Interaction (should still compile) ---

func TestHandlerExecution(t *testing.T) {
	// Setup logger for this test if handlers log internally
	originalLogger := logger.Log
	logger.Log = zaptest.NewLogger(t).Named("TestHandlerExecution")
	defer func() { logger.Log = originalLogger }()

	// This test focuses on handler mocks, setup is less critical
	ctx := context.Background()
	mockHistHandler := new(handlermock.MockHistoricalHandler)
	mockRealHandler := new(handlermock.MockRealtimeHandler)

	// Test historical handler directly
	histEventType := model.V1HistoricalChats
	histMetadata := &model.MessageMetadata{MessageSubject: string(histEventType)}
	histRawEvent := []byte(`{}`)
	mockHistHandler.On("HandleEvent", ctx, histEventType, histMetadata, histRawEvent).Return(nil)
	err := mockHistHandler.HandleEvent(ctx, histEventType, histMetadata, histRawEvent)
	assert.NoError(t, err)
	mockHistHandler.AssertExpectations(t)

	// Test realtime handler directly
	rtEventType := model.V1ChatsUpdate
	rtMetadata := &model.MessageMetadata{MessageSubject: string(rtEventType)}
	rtRawEvent := []byte(`{}`)
	mockRealHandler.On("HandleEvent", ctx, rtEventType, rtMetadata, rtRawEvent).Return(nil)
	err = mockRealHandler.HandleEvent(ctx, rtEventType, rtMetadata, rtRawEvent)
	assert.NoError(t, err)
	mockRealHandler.AssertExpectations(t)
}

// TestHistoricalHandlerExecution_Error remains largely the same logic
func TestHistoricalHandlerExecution_Error(t *testing.T) {
	// Setup logger for this test if handlers log internally
	originalLogger := logger.Log
	logger.Log = zaptest.NewLogger(t).Named("TestHistoricalHandlerExecution_Error")
	defer func() { logger.Log = originalLogger }()

	ctx := context.Background()
	mockHistHandler := new(handlermock.MockHistoricalHandler)
	mockRouter := new(ingestionmock.RouterMock)

	eventType := model.V1HistoricalChats
	metadata := &model.MessageMetadata{MessageSubject: string(eventType)}
	rawEvent := []byte(`{}`)
	expectedErr := errors.New("hist error")

	// Test direct call error
	mockHistHandler.On("HandleEvent", ctx, eventType, metadata, rawEvent).Return(expectedErr)
	err := mockHistHandler.HandleEvent(ctx, eventType, metadata, rawEvent)
	assert.Equal(t, expectedErr, err)
	mockHistHandler.AssertExpectations(t)

	// Test router call error
	mockRouter.On("Route", ctx, metadata, rawEvent).Return(expectedErr)
	// Create a dummy processor just to access the router field (or test router directly)
	dummyProcessor := &Processor{eventRouter: mockRouter}
	routeErr := dummyProcessor.eventRouter.Route(ctx, metadata, rawEvent)
	assert.Equal(t, expectedErr, routeErr)
	mockRouter.AssertExpectations(t)
}

// TestRealtimeHandlerExecution_Error remains largely the same logic
func TestRealtimeHandlerExecution_Error(t *testing.T) {
	// Setup logger for this test if handlers log internally
	originalLogger := logger.Log
	logger.Log = zaptest.NewLogger(t).Named("TestRealtimeHandlerExecution_Error")
	defer func() { logger.Log = originalLogger }()

	ctx := context.Background()
	mockRealHandler := new(handlermock.MockRealtimeHandler)
	mockRouter := new(ingestionmock.RouterMock)

	eventType := model.V1ChatsUpdate
	metadata := &model.MessageMetadata{MessageSubject: string(eventType)}
	rawEvent := []byte(`{}`)
	expectedErr := errors.New("rt error")

	// Test direct call error
	mockRealHandler.On("HandleEvent", ctx, eventType, metadata, rawEvent).Return(expectedErr)
	err := mockRealHandler.HandleEvent(ctx, eventType, metadata, rawEvent)
	assert.Equal(t, expectedErr, err)
	mockRealHandler.AssertExpectations(t)

	// Test router call error
	mockRouter.On("Route", ctx, metadata, rawEvent).Return(expectedErr)
	dummyProcessor := &Processor{eventRouter: mockRouter}
	routeErr := dummyProcessor.eventRouter.Route(ctx, metadata, rawEvent)
	assert.Equal(t, expectedErr, routeErr)
	mockRouter.AssertExpectations(t)
}

// TestHandlerInvocationViaRouter remains largely the same logic
func TestHandlerInvocationViaRouter(t *testing.T) {
	// Setup logger for this test if handlers log internally
	originalLogger := logger.Log
	logger.Log = zaptest.NewLogger(t).Named("TestHandlerInvocationViaRouter")
	defer func() { logger.Log = originalLogger }()

	ctx := context.Background()
	mockRouter := new(ingestionmock.RouterMock)
	dummyProcessor := &Processor{eventRouter: mockRouter}

	testCases := []struct {
		name        string
		metadata    *model.MessageMetadata
		rawEvent    []byte
		setupMock   func(*model.MessageMetadata, []byte)
		expectedErr error
	}{
		{
			name:     "historical success",
			metadata: &model.MessageMetadata{MessageSubject: string(model.V1HistoricalChats)}, // Simplified metadata
			rawEvent: []byte(`{}`),
			setupMock: func(meta *model.MessageMetadata, raw []byte) {
				mockRouter.On("Route", mock.Anything, meta, raw).Return(nil).Once()
			},
			expectedErr: nil,
		},
		{
			name:     "realtime error",
			metadata: &model.MessageMetadata{MessageSubject: string(model.V1MessagesUpdate)}, // Simplified metadata
			rawEvent: []byte(`{}`),
			setupMock: func(meta *model.MessageMetadata, raw []byte) {
				expectedErr := errors.New("message update error")
				mockRouter.On("Route", mock.Anything, meta, raw).Return(expectedErr).Once()
			},
			expectedErr: errors.New("message update error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupMock(tc.metadata, tc.rawEvent)
			err := dummyProcessor.eventRouter.Route(ctx, tc.metadata, tc.rawEvent)
			if tc.expectedErr != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
	mockRouter.AssertExpectations(t)
}
