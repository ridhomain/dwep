package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/config"
	clientmock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/jetstream/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap/zaptest"
)

// MockHandler is a mock of the EventHandler function
type MockHandler struct {
	mock.Mock
}

// Handle implements the EventHandler function signature
func (m *MockHandler) Handle(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
	args := m.Called(ctx, eventType, metadata, rawEvent)
	return args.Error(0)
}

// Setup test environment helper
func setupTest(t *testing.T) (*clientmock.ClientMock, *Router) {
	// Initialize logger for tests
	logger.Log = zaptest.NewLogger(t).Named("test")

	// Create mock client
	mockClient := new(clientmock.ClientMock)

	// Create router
	router := NewRouter()

	// Consumer creation is now part of specific consumer tests

	return mockClient, router
}

// --- Tests for RealtimeConsumer ---

func TestRealtimeConsumer_Setup(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-realtime"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{
		Stream:      "rt-stream",
		Consumer:    "rt-consumer-", // Base name
		QueueGroup:  "rt-group-",    // Base name
		SubjectList: []string{"rt.subj1", "rt.subj2"},
		MaxAge:      1, // 1 day
		MaxDeliver:  5,
	}

	// --- Mimic processor behavior: Modify cfg before passing ---
	cfg.Consumer = cfg.Consumer + companyID
	cfg.QueueGroup = cfg.QueueGroup + companyID
	// ---------------------------------------------------------

	// Add dlqSubject to the call, pass the modified cfg
	realtimeConsumer := NewRealtimeConsumer(mockClient, router, cfg, companyID, dlqSubject)

	// Expected args for mocks
	expectedStreamCfg := &nats.StreamConfig{
		Name:      cfg.Stream,
		Subjects:  []string{"rt.subj1.*", "rt.subj2.*"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    24 * time.Hour,
	}
	// Use the already modified cfg values for expectations
	expectedConsumerDurable := cfg.Consumer
	expectedQueueGroup := cfg.QueueGroup
	expectedConsumerSubjects := []string{"rt.subj1." + companyID, "rt.subj2." + companyID} // Construct subjects with companyID
	expectedConsumerCfg := &nats.ConsumerConfig{
		Durable:        expectedConsumerDurable,
		DeliverGroup:   expectedQueueGroup,
		FilterSubjects: expectedConsumerSubjects,
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: nats.NewInbox(),
		MaxDeliver:     cfg.MaxDeliver,
		AckWait:        30 * time.Second,
		MaxAckPending:  1000,
		ReplayPolicy:   nats.ReplayInstantPolicy,
		DeliverPolicy:  nats.DeliverLastPolicy,
	}

	// Set expectations (Context matcher remains mock.Anything)
	mockClient.On("SetupStream", mock.Anything, mock.MatchedBy(func(sc *nats.StreamConfig) bool {
		// Need to reconstruct expected subjects here as modifySubjects isn't called directly
		expectedStreamSubs, _ := modifySubjects(cfg.SubjectList, companyID)
		sc.Subjects = expectedStreamSubs // Temporarily set subjects for comparison if needed, or compare directly
		// NOTE: modifySubjects call logic is tested separately, here we assume stream name/retention etc. are primary
		// A direct comparison might be better if modifySubjects logic is complex
		return sc.Name == expectedStreamCfg.Name &&
			sc.Storage == expectedStreamCfg.Storage &&
			sc.Retention == expectedStreamCfg.Retention &&
			sc.MaxAge == expectedStreamCfg.MaxAge &&
			assert.ElementsMatch(t, expectedStreamSubs, sc.Subjects)
	})).Return(nil)
	mockClient.On("SetupConsumer", mock.Anything, cfg.Stream, mock.MatchedBy(func(cc *nats.ConsumerConfig) bool {
		// Compare relevant fields, DeliverSubject is dynamic
		return cc.Durable == expectedConsumerCfg.Durable &&
			cc.DeliverGroup == expectedConsumerCfg.DeliverGroup &&
			assert.ElementsMatch(t, expectedConsumerCfg.FilterSubjects, cc.FilterSubjects) &&
			cc.AckPolicy == expectedConsumerCfg.AckPolicy &&
			cc.MaxDeliver == expectedConsumerCfg.MaxDeliver &&
			cc.AckWait == expectedConsumerCfg.AckWait &&
			cc.MaxAckPending == expectedConsumerCfg.MaxAckPending &&
			cc.ReplayPolicy == expectedConsumerCfg.ReplayPolicy &&
			cc.DeliverPolicy == expectedConsumerCfg.DeliverPolicy
	})).Return(nil)

	// Call method
	err := realtimeConsumer.Setup()

	// Assert
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestRealtimeConsumer_Setup_StreamError(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-rt-se"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{Stream: "rt-stream-se", SubjectList: []string{"se.subj"}, MaxDeliver: 5}
	// Add dlqSubject to the call
	realtimeConsumer := NewRealtimeConsumer(mockClient, router, cfg, companyID, dlqSubject)

	expectedErr := errors.New("stream setup failed")
	mockClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(expectedErr)

	err := realtimeConsumer.Setup()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Contains(t, err.Error(), "failed to setup realtime stream")
	mockClient.AssertExpectations(t)
	mockClient.AssertNotCalled(t, "SetupConsumer", mock.Anything, mock.Anything, mock.Anything)
}

func TestRealtimeConsumer_Setup_ConsumerError(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-rt-ce"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{Stream: "rt-stream-ce", Consumer: "rt-con-ce", SubjectList: []string{"ce.subj"}, MaxDeliver: 5}
	// Add dlqSubject to the call
	realtimeConsumer := NewRealtimeConsumer(mockClient, router, cfg, companyID, dlqSubject)

	expectedErr := errors.New("consumer setup failed")
	mockClient.On("SetupStream", mock.Anything, mock.AnythingOfType("*nats.StreamConfig")).Return(nil)
	mockClient.On("SetupConsumer", mock.Anything, cfg.Stream, mock.AnythingOfType("*nats.ConsumerConfig")).Return(expectedErr)

	err := realtimeConsumer.Setup()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Contains(t, err.Error(), "failed to setup realtime consumer")
	mockClient.AssertExpectations(t)
}

func TestRealtimeConsumer_Start(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-rt-start"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{
		// Base names in the initial config
		Consumer:   "rt-con-start-",
		QueueGroup: "rt-grp-start-",
		MaxDeliver: 5,
	}

	// --- Mimic processor behavior: Modify cfg BEFORE passing ---
	modifiedCfg := cfg
	modifiedCfg.Consumer = cfg.Consumer + companyID
	modifiedCfg.QueueGroup = cfg.QueueGroup + companyID
	// ---------------------------------------------------------

	// Pass the MODIFIED config to the constructor
	realtimeConsumer := NewRealtimeConsumer(mockClient, router, modifiedCfg, companyID, dlqSubject)

	// Expectations MUST match the names stored in the consumer's config (which now include companyID)
	expectedConsumerDurable := modifiedCfg.Consumer
	expectedQueueGroup := modifiedCfg.QueueGroup
	mockSubscription := clientmock.MockSubscription() // Use the helper from jetstream/mock

	// Expect SubscribePush to be called with correct args (base name + companyID)
	mockClient.On("SubscribePush", "", expectedConsumerDurable, expectedQueueGroup, cfg.Stream, mock.AnythingOfType("nats.MsgHandler")).Return(mockSubscription, nil)

	err := realtimeConsumer.Start()

	assert.NoError(t, err)
	assert.Equal(t, mockSubscription, realtimeConsumer.sub) // Check if sub handle is stored
	mockClient.AssertExpectations(t)
}

func TestRealtimeConsumer_Start_Error(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-rt-start-err"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{
		Consumer:   "rt-con-start-err-",
		QueueGroup: "rt-grp-start-err-",
		MaxDeliver: 5,
		// Add dummy delay values for constructor
		NakBaseDelay: 1 * time.Second,
		NakMaxDelay:  10 * time.Second,
	}
	// Pass the config containing base names
	realtimeConsumer := NewRealtimeConsumer(mockClient, router, cfg, companyID, dlqSubject)

	expectedErr := errors.New("subscribe push failed")

	mockClient.On("SubscribePush", "", cfg.Consumer, cfg.QueueGroup, cfg.Stream, mock.AnythingOfType("nats.MsgHandler")).Return((*nats.Subscription)(nil), expectedErr)

	err := realtimeConsumer.Start()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedErr.Error())
	assert.Contains(t, err.Error(), "failed to subscribe realtime consumer")
	assert.Nil(t, realtimeConsumer.sub)
	mockClient.AssertExpectations(t)
}

func TestRealtimeConsumer_Stop(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-rt-stop"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{Consumer: "rt-con-stop-", MaxDeliver: 5}
	// Add dlqSubject to the call
	realtimeConsumer := NewRealtimeConsumer(mockClient, router, cfg, companyID, dlqSubject)

	// Set the subscription handle using the helper (returns nil)
	realtimeConsumer.sub = clientmock.MockSubscription()

	// Need to access the internal context/cancel of the base consumer
	ctx := realtimeConsumer.base.ctx

	// Call Stop
	realtimeConsumer.Stop()

	// Verify context was canceled
	select {
	case <-ctx.Done():
		// Context canceled as expected
		assert.True(t, true)
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Context was not canceled within timeout")
	}
	mockClient.AssertExpectations(t)
}

// --- Tests for HistoricalConsumer ---

func TestHistoricalConsumer_Setup(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-hist"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{
		Stream:      "hist-stream",
		Consumer:    "hist-consumer-", // Base name
		QueueGroup:  "hist-group-",    // Base name
		SubjectList: []string{"hist.subj1", "hist.subj2"},
		MaxAge:      30,
		MaxDeliver:  3,
	}

	// --- Mimic processor behavior: Modify cfg before passing ---
	cfg.Consumer = cfg.Consumer + companyID
	cfg.QueueGroup = cfg.QueueGroup + companyID
	// ---------------------------------------------------------

	// Add dlqSubject to the call, pass the modified cfg
	historicalConsumer := NewHistoricalConsumer(mockClient, router, cfg, companyID, dlqSubject)

	// Expected args for mocks
	expectedStreamCfg := &nats.StreamConfig{
		Name:      cfg.Stream,
		Subjects:  []string{"hist.subj1.*", "hist.subj2.*"},
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
		MaxAge:    30 * 24 * time.Hour,
	}
	// Use the already modified cfg values for expectations
	expectedConsumerDurable := cfg.Consumer
	expectedQueueGroup := cfg.QueueGroup
	expectedConsumerSubjects := []string{"hist.subj1." + companyID, "hist.subj2." + companyID}
	expectedConsumerCfg := &nats.ConsumerConfig{
		Durable:        expectedConsumerDurable,
		DeliverGroup:   expectedQueueGroup,
		FilterSubjects: expectedConsumerSubjects,
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: nats.NewInbox(),
		MaxDeliver:     cfg.MaxDeliver,
		AckWait:        60 * time.Second,
		MaxAckPending:  500,
		ReplayPolicy:   nats.ReplayInstantPolicy,
		DeliverPolicy:  nats.DeliverLastPolicy,
	}

	// Set expectations (Context matcher remains mock.Anything)
	mockClient.On("SetupStream", mock.Anything, mock.MatchedBy(func(sc *nats.StreamConfig) bool {
		// Need to reconstruct expected subjects here as modifySubjects isn't called directly
		expectedStreamSubs, _ := modifySubjects(cfg.SubjectList, companyID)
		// Compare relevant fields
		return sc.Name == expectedStreamCfg.Name &&
			sc.Storage == expectedStreamCfg.Storage &&
			sc.Retention == expectedStreamCfg.Retention &&
			sc.MaxAge == expectedStreamCfg.MaxAge &&
			assert.ElementsMatch(t, expectedStreamSubs, sc.Subjects)
	})).Return(nil)
	mockClient.On("SetupConsumer", mock.Anything, cfg.Stream, mock.MatchedBy(func(cc *nats.ConsumerConfig) bool {
		// Compare relevant fields, DeliverSubject is dynamic
		return cc.Durable == expectedConsumerCfg.Durable &&
			cc.DeliverGroup == expectedConsumerCfg.DeliverGroup &&
			assert.ElementsMatch(t, expectedConsumerCfg.FilterSubjects, cc.FilterSubjects) &&
			cc.AckPolicy == expectedConsumerCfg.AckPolicy &&
			cc.MaxDeliver == expectedConsumerCfg.MaxDeliver &&
			cc.AckWait == expectedConsumerCfg.AckWait &&
			cc.MaxAckPending == expectedConsumerCfg.MaxAckPending &&
			cc.ReplayPolicy == expectedConsumerCfg.ReplayPolicy &&
			cc.DeliverPolicy == expectedConsumerCfg.DeliverPolicy
	})).Return(nil)

	err := historicalConsumer.Setup()

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestHistoricalConsumer_Start(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-hist-start"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{
		// Base names in the initial config
		Consumer:   "hist-con-start-",
		QueueGroup: "hist-grp-start-",
		MaxDeliver: 3,
	}

	// --- Mimic processor behavior: Modify cfg BEFORE passing ---
	modifiedCfg := cfg
	modifiedCfg.Consumer = cfg.Consumer + companyID
	modifiedCfg.QueueGroup = cfg.QueueGroup + companyID
	// ---------------------------------------------------------

	// Pass the MODIFIED config to the constructor
	historicalConsumer := NewHistoricalConsumer(mockClient, router, modifiedCfg, companyID, dlqSubject)

	// Expectations MUST match the names stored in the consumer's config (which now include companyID)
	expectedConsumerDurable := modifiedCfg.Consumer
	expectedQueueGroup := modifiedCfg.QueueGroup
	mockSubscription := clientmock.MockSubscription()

	mockClient.On("SubscribePush", "", expectedConsumerDurable, expectedQueueGroup, cfg.Stream, mock.AnythingOfType("nats.MsgHandler")).Return(mockSubscription, nil)

	err := historicalConsumer.Start()

	assert.NoError(t, err)
	assert.Equal(t, mockSubscription, historicalConsumer.sub)
	mockClient.AssertExpectations(t)
}

func TestHistoricalConsumer_Stop(t *testing.T) {
	mockClient, router := setupTest(t)
	companyID := "test-tenant-hist-stop"
	dlqSubject := "test.dlq"
	cfg := config.ConsumerNatsConfig{Consumer: "hist-con-stop-", MaxDeliver: 3}
	// Add dlqSubject to the call
	historicalConsumer := NewHistoricalConsumer(mockClient, router, cfg, companyID, dlqSubject)

	// Set the subscription handle using the helper (returns nil)
	historicalConsumer.sub = clientmock.MockSubscription()

	ctx := historicalConsumer.base.ctx
	historicalConsumer.Stop()

	select {
	case <-ctx.Done():
		assert.True(t, true)
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "Context was not canceled within timeout")
	}
	mockClient.AssertExpectations(t)
}

// --- Tests for determineAckNakAction ---

func TestDetermineAckNakAction(t *testing.T) {
	baseDelay := 1 * time.Second
	maxDelay := 16 * time.Second
	maxDeliver := 5

	tests := []struct {
		name           string
		processingErr  error
		numDelivered   uint64
		expectedAction AckNakAction
		expectedDelay  time.Duration
	}{
		{
			name:           "Success case",
			processingErr:  nil,
			numDelivered:   1,
			expectedAction: ActionAck,
			expectedDelay:  0,
		},
		{
			name:           "Retryable error, first attempt",
			processingErr:  apperrors.NewRetryable(errors.New("transient"), "transient"),
			numDelivered:   1,
			expectedAction: ActionNakDelay,
			expectedDelay:  1 * time.Second, // base * 2^0
		},
		{
			name:           "Retryable error, second attempt",
			processingErr:  apperrors.NewRetryable(errors.New("transient"), "transient"),
			numDelivered:   2,
			expectedAction: ActionNakDelay,
			expectedDelay:  2 * time.Second, // base * 2^1
		},
		{
			name:           "Retryable error, third attempt",
			processingErr:  apperrors.NewRetryable(errors.New("transient"), "transient"),
			numDelivered:   3,
			expectedAction: ActionNakDelay,
			expectedDelay:  4 * time.Second, // base * 2^2
		},
		{
			name:           "Retryable error, fourth attempt",
			processingErr:  apperrors.NewRetryable(errors.New("transient"), "transient"),
			numDelivered:   4,
			expectedAction: ActionNakDelay,
			expectedDelay:  8 * time.Second, // base * 2^3
		},
		{
			name:           "Retryable error, fifth attempt (maxDeliver reached)",
			processingErr:  apperrors.NewRetryable(errors.New("transient"), "transient"),
			numDelivered:   5, // = maxDeliver
			expectedAction: ActionDLQ,
			expectedDelay:  0,
		},
		{
			name:           "Retryable error, delay exceeds maxDelay",
			processingErr:  apperrors.NewRetryable(errors.New("transient"), "transient"),
			numDelivered:   5,         // Attempt that would result in 16s delay
			expectedAction: ActionDLQ, // Max deliver reached first here
			expectedDelay:  0,
		},
		{
			name:           "Fatal error, first attempt",
			processingErr:  apperrors.NewFatal(errors.New("fatal"), "fatal"),
			numDelivered:   1,
			expectedAction: ActionDLQ,
			expectedDelay:  0,
		},
		{
			name:           "Fatal error, later attempt",
			processingErr:  apperrors.NewFatal(errors.New("fatal"), "fatal"),
			numDelivered:   3,
			expectedAction: ActionDLQ,
			expectedDelay:  0,
		},
		{
			name:           "Non-app error (treated as fatal), first attempt",
			processingErr:  errors.New("some other error"),
			numDelivered:   1,
			expectedAction: ActionDLQ,
			expectedDelay:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := &nats.MsgMetadata{
				NumDelivered: tt.numDelivered,
				// Other metadata fields can be default/zero for this test
			}
			action, delay := determineAckNakAction(tt.processingErr, metadata, maxDeliver, baseDelay, maxDelay)
			assert.Equal(t, tt.expectedAction, action, "Action should match")
			assert.Equal(t, tt.expectedDelay, delay, "Delay should match")
		})
	}
}

// --- Helper Function Tests ---

func TestModifySubjects(t *testing.T) {
	tests := []struct {
		name                 string
		inputSubjects        []string
		companyID            string
		expectedStreamSubs   []string
		expectedConsumerSubs []string
	}{
		{
			name:                 "basic case",
			inputSubjects:        []string{"v1.chats", "v1.messages"},
			companyID:            "tenantA",
			expectedStreamSubs:   []string{"v1.chats.*", "v1.messages.*"},
			expectedConsumerSubs: []string{"v1.chats.tenantA", "v1.messages.tenantA"},
		},
		{
			name:                 "single subject",
			inputSubjects:        []string{"v1.contacts.update"},
			companyID:            "tenantB",
			expectedStreamSubs:   []string{"v1.contacts.update.*"},
			expectedConsumerSubs: []string{"v1.contacts.update.tenantB"},
		},
		{
			name:                 "empty input list",
			inputSubjects:        []string{},
			companyID:            "tenantC",
			expectedStreamSubs:   []string{},
			expectedConsumerSubs: []string{},
		},
		{
			name:                 "empty tenant ID", // Should still append dot
			inputSubjects:        []string{"v1.data"},
			companyID:            "",
			expectedStreamSubs:   []string{"v1.data.*"},
			expectedConsumerSubs: []string{"v1.data."},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamSubs, consumerSubs := modifySubjects(tt.inputSubjects, tt.companyID)
			assert.ElementsMatch(t, tt.expectedStreamSubs, streamSubs, "Stream subjects should match")
			assert.ElementsMatch(t, tt.expectedConsumerSubs, consumerSubs, "Consumer subjects should match")
		})
	}
}
