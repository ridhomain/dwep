package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	storagemock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

func init() {
	// Initialize logger for tests
	logger.Log = zaptest.NewLogger(nil).Named("test")
}

// TestProcessHistoricalChatsEmptyID tests handling of chats with empty IDs
func TestProcessHistoricalChatsEmptyID(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data
	companyID := "company-1"
	chatPayloads := []model.UpsertChatPayload{
		{
			ChatID:      "",
			Jid:         "jid-1",
			CompanyID:   companyID,
			PhoneNumber: "+11111111111",
		},
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.ProcessHistoricalChats(ctx, chatPayloads, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "validation failed for historical chat at index 0")

	// Verify logs - there should be an error log for validation failure
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Validation failed for historical chat" {
			foundError = true
			assert.Equal(t, "", log.ContextMap()["chat_id"])
			assert.Contains(t, log.ContextMap()["error"].(string), "required")
			break
		}
	}
	assert.True(t, foundError, "Expected error log about validation failure")

	// No repository calls should happen due to validation error
	chatRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
}

// TestProcessHistoricalChatsEmptySlice ensures that empty slice of chats is handled correctly
func TestProcessHistoricalChatsEmptySlice(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.WarnLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "company-1")

	// Call function with empty slice
	err := service.ProcessHistoricalChats(ctx, []model.UpsertChatPayload{}, nil)

	// Assert
	assert.NoError(t, err, "Expected no error for empty slice")
	assert.Equal(t, 1, observedLogs.Len(), "Expected one log message")
	assert.Equal(t, "No chats to process in historical chats payload", observedLogs.All()[0].Message)

	// Verify no calls to repository methods
	chatRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
}

// TestProcessHistoricalChatsCompanyError tests handling of errors when extracting company from context
func TestProcessHistoricalChatsCompanyError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data - valid payload but context without company
	chatPayload := model.UpsertChatPayload{
		ChatID:      "chat-1",
		Jid:         "jid-1",
		CompanyID:   "company-id-placeholder",
		PhoneNumber: "+1111111111",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	// Intentionally NOT adding company to context

	// Call function
	err := service.ProcessHistoricalChats(ctx, []model.UpsertChatPayload{chatPayload}, nil)

	// Assert
	assert.Error(t, err, "Expected error when company missing from context")
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.ErrorIs(t, fatalErr.Unwrap(), tenant.ErrCompanyIDNotFound)

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Failed to get tenant ID from context for historical chats" {
			foundError = true
			break
		}
	}
	assert.True(t, foundError, "Expected error log about missing company")

	// Verify no repository calls should happen
	chatRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
}

func TestProcessHistoricalChats(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data
	companyID := "company-1"
	chatPayloads := []model.UpsertChatPayload{
		{
			ChatID:                "chat-1",
			Jid:                   "jid-1",
			CompanyID:             companyID,
			PhoneNumber:           "+111111111",
			PushName:              "Push Name 1",
			GroupName:             "Group 1",
			ConversationTimestamp: int64(time.Now().Unix()),
			UnreadCount:           5,
			IsGroup:               true,
			NotSpam:               true,
		},
		{
			ChatID:                "chat-2",
			Jid:                   "jid-2",
			CompanyID:             companyID,
			PhoneNumber:           "+1234567890",
			PushName:              "Push Name 2",
			ConversationTimestamp: int64(time.Now().Unix()),
			UnreadCount:           0,
			IsGroup:               false,
			NotSpam:               true,
		},
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 123,
		StreamSequence:   456,
		Stream:           "stream-1",
		Consumer:         "consumer-1",
		Domain:           "domain-1",
		MessageID:        "msg-1",
		MessageSubject:   "subj-1",
		CompanyID:        companyID,
	}

	// Setup expectations
	chatRepo.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Chat")).Return(nil)

	// Setup context with company and logger
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.ProcessHistoricalChats(ctx, chatPayloads, metadata)

	// Assert
	assert.NoError(t, err)
	chatRepo.AssertCalled(t, "BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Chat"))

	// Get the actual chats passed to BulkUpsert
	calls := chatRepo.Calls
	require.GreaterOrEqual(t, len(calls), 1)
	chats := calls[len(calls)-1].Arguments.Get(1).([]model.Chat)

	// Verify the transformed data
	assert.Len(t, chats, 2)
	assert.Equal(t, "chat-1", chats[0].ChatID)
	assert.Equal(t, companyID, chats[0].CompanyID)
	assert.Equal(t, "chat-2", chats[1].ChatID)
	assert.Equal(t, companyID, chats[1].CompanyID)
}

func TestProcessHistoricalChatsWithValidationErrors(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data with empty ID (will fail validation)
	chatPayloads := []model.UpsertChatPayload{
		{
			ChatID:      "",
			Jid:         "jid-1",
			CompanyID:   "company-1",
			PhoneNumber: "+1111111111",
		},
	}

	// Setup context with company and logger
	ctx := tenant.WithCompanyID(context.Background(), "company-1")
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.ProcessHistoricalChats(ctx, chatPayloads, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "validation failed for historical chat at index 0")
	chatRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
}

func TestProcessHistoricalChatsWithRepoError(t *testing.T) {
	// Table-driven test for different repo errors
	testCases := []struct {
		name            string
		repoErr         error
		expectRetryable bool
		expectFatal     bool
		originalErr     error
		logLevel        zapcore.Level
		logMessage      string
	}{
		{
			name:            "Retryable DB Error",
			repoErr:         fmt.Errorf("bulk db fail: %w", apperrors.ErrDatabase),
			expectRetryable: true,
			expectFatal:     false,
			originalErr:     apperrors.ErrDatabase,
			logLevel:        zapcore.WarnLevel,
			logMessage:      "Potentially retryable error during historical chats bulk upsert",
		},
		{
			name:            "Fatal Bad Request Error", // Example of fatal error from repo
			repoErr:         fmt.Errorf("bad data in bulk: %w", apperrors.ErrBadRequest),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrBadRequest,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error during historical chats bulk upsert",
		},
		{
			name:            "Fatal Other Error",
			repoErr:         errors.New("unexpected bulk failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error during historical chats bulk upsert",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			chatRepo := new(storagemock.ChatRepoMock)
			messageRepo := new(storagemock.MessageRepoMock)
			contactRepo := new(storagemock.ContactRepoMock)
			agentRepo := new(storagemock.AgentRepoMock)
			onboardingRepo := new(storagemock.OnboardingLogRepoMock)
			exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
			onboardingWorker := new(MockOnboardingWorker)
			service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

			companyID := "company-1"
			chatPayloads := []model.UpsertChatPayload{
				{ChatID: "chat-1", Jid: "jid-1", CompanyID: companyID, PhoneNumber: "+1111111111"},
			}

			// Setup expectations - use the error from the test case
			chatRepo.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Chat")).Return(tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zapcore.WarnLevel) // Warn or Error
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.ProcessHistoricalChats(ctx, chatPayloads, nil)

			// Assert
			assert.Error(t, err)
			chatRepo.AssertCalled(t, "BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Chat"))

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, retryableErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
			} else if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, fatalErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
			}

			// Check logs
			require.GreaterOrEqual(t, observedLogs.Len(), 1)
			assert.Equal(t, tc.logLevel, observedLogs.All()[0].Level)
			assert.Equal(t, tc.logMessage, observedLogs.All()[0].Message)
		})
	}
}

func TestUpsertChat(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data
	companyID := "company-1"
	chatPayload := model.UpsertChatPayload{
		ChatID:                "chat-1",
		Jid:                   "jid-1",
		CompanyID:             companyID,
		PhoneNumber:           "+1111111111",
		PushName:              "Push Name 1",
		ConversationTimestamp: int64(time.Now().Unix()),
		UnreadCount:           5,
		IsGroup:               false,
		NotSpam:               true,
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 123,
		StreamSequence:   456,
		Stream:           "stream-1",
		Consumer:         "consumer-1",
		MessageID:        "msg-1",
		MessageSubject:   "subj-1",
		CompanyID:        companyID,
	}

	// Setup expectations
	chatRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Chat")).Return(nil)

	// Setup context with company and logger
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.UpsertChat(ctx, chatPayload, metadata)

	// Assert
	assert.NoError(t, err)
	chatRepo.AssertCalled(t, "Save", mock.Anything, mock.AnythingOfType("model.Chat"))

	// Get the actual chat passed to Save
	calls := chatRepo.Calls
	require.GreaterOrEqual(t, len(calls), 1)
	chat := calls[len(calls)-1].Arguments.Get(1).(model.Chat)

	// Verify the transformed data
	assert.Equal(t, "chat-1", chat.ChatID)
	assert.Equal(t, companyID, chat.CompanyID)
	assert.Equal(t, int32(5), chat.UnreadCount)
	assert.False(t, chat.IsGroup)
	assert.True(t, chat.NotSpam)
}

// TestUpsertChatValidationError tests that validation errors in UpsertChat are handled correctly
func TestUpsertChatValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup invalid test data (missing required ChatID field)
	chatPayload := model.UpsertChatPayload{
		Jid:         "jid-1",
		CompanyID:   "company-1",
		PhoneNumber: "+1111111111",
		PushName:    "Push Name 1",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "company-1")

	// Call function
	err := service.UpsertChat(ctx, chatPayload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "chat validation failed")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Chat validation failed" {
			foundError = true
			assert.Contains(t, log.ContextMap()["error"].(string), "chat_id")
			break
		}
	}
	assert.True(t, foundError, "Expected error log about validation failure")

	// Verify no repository calls were made
	chatRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

// TestUpsertChatCompanyError tests handling of errors when extracting company from context
func TestUpsertChatCompanyError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data
	chatPayload := model.UpsertChatPayload{
		ChatID:      "chat-1",
		Jid:         "jid-1",
		CompanyID:   "company-id",
		PhoneNumber: "+1111111111",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	// Intentionally NOT adding company to context

	// Call function
	err := service.UpsertChat(ctx, chatPayload, nil)

	// Assert
	assert.Error(t, err, "Expected error when company missing from context")
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "failed to get tenant ID from context")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Failed to get tenant ID from context for upsert chat" {
			foundError = true
			break
		}
	}
	assert.True(t, foundError, "Expected error log about missing company")

	// Verify no repository calls
	chatRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

// TestUpsertChatRepositoryError tests handling of repository errors
func TestUpsertChatRepositoryError(t *testing.T) {
	// Table-driven test for different repo errors
	testCases := []struct {
		name            string
		repoErr         error
		expectRetryable bool
		expectFatal     bool
		originalErr     error
		logLevel        zapcore.Level
		logMessage      string
	}{
		{
			name:            "Retryable DB Error",
			repoErr:         fmt.Errorf("save db fail: %w", apperrors.ErrDatabase),
			expectRetryable: true,
			expectFatal:     false,
			originalErr:     apperrors.ErrDatabase,
			logLevel:        zapcore.WarnLevel,
			logMessage:      "Potentially retryable error during chat upsert",
		},
		{
			name:            "Fatal Duplicate Error",
			repoErr:         fmt.Errorf("chat exists: %w", apperrors.ErrDuplicate),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrDuplicate,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error during chat upsert",
		},
		{
			name:            "Fatal Bad Request Error",
			repoErr:         fmt.Errorf("bad data: %w", apperrors.ErrBadRequest),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrBadRequest,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error during chat upsert",
		},
		{
			name:            "Fatal Other Error",
			repoErr:         errors.New("unexpected save failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error during chat upsert",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			chatRepo := new(storagemock.ChatRepoMock)
			messageRepo := new(storagemock.MessageRepoMock)
			contactRepo := new(storagemock.ContactRepoMock)
			agentRepo := new(storagemock.AgentRepoMock)
			onboardingRepo := new(storagemock.OnboardingLogRepoMock)
			exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
			onboardingWorker := new(MockOnboardingWorker)
			service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

			companyID := "company-1"
			chatPayload := model.UpsertChatPayload{
				ChatID: "chat-1", Jid: "jid-1", CompanyID: companyID, PhoneNumber: "+1111111111",
			}

			// Setup expectations
			chatRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Chat")).Return(tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zapcore.WarnLevel)
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.UpsertChat(ctx, chatPayload, nil)

			// Assert
			assert.Error(t, err)
			chatRepo.AssertCalled(t, "Save", mock.Anything, mock.AnythingOfType("model.Chat"))

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, retryableErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
			} else if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, fatalErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
			}

			// Check logs
			require.GreaterOrEqual(t, observedLogs.Len(), 1)
			assert.Equal(t, tc.logLevel, observedLogs.All()[0].Level)
			assert.Equal(t, tc.logMessage, observedLogs.All()[0].Message)
		})
	}
}

func TestUpdateChat(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data
	companyID := "company-1"
	agentID := "agemt-1"
	updatePayload := model.UpdateChatPayload{
		ChatID:                "chat-1",
		UnreadCount:           10,
		ConversationTimestamp: 1623456789,
		CompanyID:             companyID,
		AgentID:               agentID,
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 124,
		StreamSequence:   457,
		MessageID:        "msg-2",
	}

	// Create an existing chat to be returned by FindChatByChatID
	existingChat := &model.Chat{
		ID:                    1,
		ChatID:                "chat-1",
		Jid:                   "jid-1",
		CompanyID:             companyID,
		UnreadCount:           5,
		ConversationTimestamp: 1623456000,
		CreatedAt:             time.Now().Add(-24 * time.Hour),
		UpdatedAt:             time.Now().Add(-1 * time.Hour),
	}

	// Setup expectations
	chatRepo.On("FindChatByChatID", mock.Anything, "chat-1").Return(existingChat, nil)
	chatRepo.On("Update", mock.Anything, mock.AnythingOfType("model.Chat")).Return(nil)

	// Setup context with company and logger
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.UpdateChat(ctx, updatePayload, metadata)

	// Assert
	assert.NoError(t, err)
	chatRepo.AssertCalled(t, "FindChatByChatID", mock.Anything, "chat-1")
	chatRepo.AssertCalled(t, "Update", mock.Anything, mock.AnythingOfType("model.Chat"))

	// Get the actual chat passed to Update
	calls := chatRepo.Calls
	require.GreaterOrEqual(t, len(calls), 2)
	chat := calls[len(calls)-1].Arguments.Get(1).(model.Chat)

	// Verify the updated data
	assert.Equal(t, int64(1), chat.ID)
	assert.Equal(t, "chat-1", chat.ChatID)
	assert.Equal(t, companyID, chat.CompanyID)
	assert.Equal(t, int32(15), chat.UnreadCount)
	assert.Equal(t, int64(1623456789), chat.ConversationTimestamp)
	assert.NotEqual(t, existingChat.UpdatedAt, chat.UpdatedAt)
}

func TestUpdateChatWithCompanyMismatch(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data
	updatePayload := model.UpdateChatPayload{
		ChatID:      "chat-1",
		UnreadCount: 10,
		CompanyID:   "company-1",
		AgentID:     "agent-1",
	}

	// Create an existing chat with different company id
	existingChat := &model.Chat{
		ID:          1,
		ChatID:      "chat-1",
		Jid:         "jid-1",
		CompanyID:   "company-2",
		UnreadCount: 5,
	}

	// Setup expectations
	chatRepo.On("FindChatByChatID", mock.Anything, "chat-1").Return(existingChat, nil)

	// Setup context with company and logger
	ctx := tenant.WithCompanyID(context.Background(), "company-1")
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.UpdateChat(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "company ID mismatch for chat chat-1")
	chatRepo.AssertCalled(t, "FindChatByChatID", mock.Anything, "chat-1")
	chatRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

// TestUpdateChatFindByChatIDError tests handling of errors when fetching existing chat
func TestUpdateChatFindByChatIDError(t *testing.T) {
	// Table-driven test for different FindByChatID errors
	testCases := []struct {
		name            string
		repoErr         error
		expectRetryable bool
		expectFatal     bool
		originalErr     error
		logLevel        zapcore.Level
		logMessage      string
	}{
		{
			name:            "Retryable DB Error on Find",
			repoErr:         fmt.Errorf("find network issue: %w", apperrors.ErrDatabase),
			expectRetryable: true,
			expectFatal:     false,
			originalErr:     apperrors.ErrDatabase,
			logLevel:        zapcore.WarnLevel,
			logMessage:      "Potentially retryable error fetching chat for update",
		},
		{
			name:            "Fatal Not Found Error on Find",
			repoErr:         apperrors.ErrNotFound, // Repo returns this directly
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrNotFound,
			logLevel:        zapcore.WarnLevel,
			logMessage:      "Chat not found for update",
		},
		{
			name:            "Fatal Other Error on Find",
			repoErr:         errors.New("unexpected find failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error fetching chat for update",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			chatRepo := new(storagemock.ChatRepoMock)
			messageRepo := new(storagemock.MessageRepoMock)
			contactRepo := new(storagemock.ContactRepoMock)
			agentRepo := new(storagemock.AgentRepoMock)
			onboardingRepo := new(storagemock.OnboardingLogRepoMock)
			exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
			onboardingWorker := new(MockOnboardingWorker)
			service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

			companyID := "company-1"
			chatID := "chat-find-err"
			updatePayload := model.UpdateChatPayload{ChatID: chatID, UnreadCount: 10, CompanyID: companyID}

			// Setup expectations
			chatRepo.On("FindChatByChatID", mock.Anything, chatID).Return(nil, tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zapcore.WarnLevel)
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.UpdateChat(ctx, updatePayload, nil)

			// Assert
			assert.Error(t, err)
			chatRepo.AssertCalled(t, "FindChatByChatID", mock.Anything, chatID)
			chatRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, retryableErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
			} else if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, fatalErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
			}

			// Check logs
			require.GreaterOrEqual(t, observedLogs.Len(), 1)
			assert.Equal(t, tc.logLevel, observedLogs.All()[0].Level)
			assert.Equal(t, tc.logMessage, observedLogs.All()[0].Message)
		})
	}
}

// TestUpdateChatChatNotFound tests handling of non-existent chat
func TestUpdateChatChatNotFound(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data
	companyID := "company-1"
	updatePayload := model.UpdateChatPayload{
		ChatID:                "non-existent-chat",
		UnreadCount:           10,
		ConversationTimestamp: 1623456789,
		CompanyID:             companyID,
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.WarnLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Setup expectations - FindChatByChatID returns apperrors.ErrNotFound
	chatRepo.On("FindChatByChatID", mock.Anything, "non-existent-chat").Return(nil, apperrors.ErrNotFound)

	// Call function
	err := service.UpdateChat(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err, "Expected error when chat not found")
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "chat not found for update")
	assert.ErrorIs(t, fatalErr.Unwrap(), apperrors.ErrNotFound)

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundWarning bool
	for _, log := range observedLogs.All() {
		if log.Message == "Chat not found for update" {
			foundWarning = true
			assert.Equal(t, "non-existent-chat", log.ContextMap()["chat_id"])
			break
		}
	}
	assert.True(t, foundWarning, "Expected warning log about chat not found")

	// Verify repository calls
	chatRepo.AssertCalled(t, "FindChatByChatID", mock.Anything, "non-existent-chat")
	chatRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

// TestUpdateChatUpdateError tests handling of errors when updating chat
func TestUpdateChatUpdateError(t *testing.T) {
	// Table-driven test for different Update errors
	testCases := []struct {
		name            string
		repoErr         error
		expectRetryable bool
		expectFatal     bool
		originalErr     error
		logLevel        zapcore.Level
		logMessage      string
	}{
		{
			name:            "Retryable DB Error on Update",
			repoErr:         fmt.Errorf("update db fail: %w", apperrors.ErrDatabase),
			expectRetryable: true,
			expectFatal:     false,
			originalErr:     apperrors.ErrDatabase,
			logLevel:        zapcore.WarnLevel,
			logMessage:      "Potentially retryable error during chat update",
		},
		{
			name:            "Fatal Bad Request on Update",
			repoErr:         fmt.Errorf("invalid update data: %w", apperrors.ErrBadRequest),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrBadRequest,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error during chat update",
		},
		{
			name:            "Fatal Other Error on Update",
			repoErr:         errors.New("unexpected update failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error during chat update",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			chatRepo := new(storagemock.ChatRepoMock)
			messageRepo := new(storagemock.MessageRepoMock)
			contactRepo := new(storagemock.ContactRepoMock)
			agentRepo := new(storagemock.AgentRepoMock)
			onboardingRepo := new(storagemock.OnboardingLogRepoMock)
			exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
			onboardingWorker := new(MockOnboardingWorker)
			service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

			companyID := "company-1"
			chatID := "chat-update-repo-err"
			updatePayload := model.UpdateChatPayload{ChatID: chatID, UnreadCount: 10, CompanyID: companyID}
			existingChat := &model.Chat{ID: 1, ChatID: chatID, Jid: "jid-update", CompanyID: companyID}

			// Setup expectations
			chatRepo.On("FindChatByChatID", mock.Anything, chatID).Return(existingChat, nil)
			chatRepo.On("Update", mock.Anything, mock.AnythingOfType("model.Chat")).Return(tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zapcore.WarnLevel)
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.UpdateChat(ctx, updatePayload, nil)

			// Assert
			assert.Error(t, err)
			chatRepo.AssertCalled(t, "FindChatByChatID", mock.Anything, chatID)
			chatRepo.AssertCalled(t, "Update", mock.Anything, mock.AnythingOfType("model.Chat"))

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, retryableErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
			} else if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, fatalErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
			}

			// Check logs
			require.GreaterOrEqual(t, observedLogs.Len(), 1)
			assert.Equal(t, tc.logLevel, observedLogs.All()[0].Level)
			assert.Equal(t, tc.logMessage, observedLogs.All()[0].Message)
		})
	}
}
