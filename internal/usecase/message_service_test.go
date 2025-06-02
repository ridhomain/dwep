package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	storagemock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

// Mock OnboardingWorker
type MockOnboardingWorker struct {
	mock.Mock
}

func (m *MockOnboardingWorker) SubmitTask(taskData OnboardingTaskData) error {
	args := m.Called(taskData)
	return args.Error(0)
}

func (m *MockOnboardingWorker) Stop() {
	m.Called()
}

// Helper function to setup mocks and service for tests
func setupTestServiceWithMocks(t *testing.T) (*EventService, *storagemock.ChatRepoMock, *storagemock.MessageRepoMock, *storagemock.ContactRepoMock, *storagemock.AgentRepoMock, *storagemock.OnboardingLogRepoMock, *storagemock.ExhaustedEventRepoMock, *MockOnboardingWorker) {
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)

	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)
	return service, chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker
}

// Ensure MockOnboardingWorker implements IOnboardingWorker (compile-time check)
var _ IOnboardingWorker = (*MockOnboardingWorker)(nil)

func init() {
	// Initialize logger for tests
	logger.Log = zaptest.NewLogger(nil).Named("test")
}

// TestProcessHistoricalMessagesEmptySlice ensures that empty slice of messages is handled correctly
func TestProcessHistoricalMessagesEmptySlice(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.WarnLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "tenant-1")

	// Call function with empty slice
	err := service.ProcessHistoricalMessages(ctx, []model.UpsertMessagePayload{}, nil)

	// Assert
	assert.NoError(t, err, "Expected no error for empty slice")
	assert.Equal(t, 1, observedLogs.Len(), "Expected one log message")
	assert.Equal(t, "No messages to process in historical messages payload", observedLogs.All()[0].Message)

	// Verify no calls to repository methods or worker
	messageRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestProcessHistoricalMessagesValidationError tests handling of validation errors
func TestProcessHistoricalMessagesValidationError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data - message without required fields to fail validation
	companyID := "tenant-1"
	messagePayload := model.UpsertMessagePayload{
		// Missing MessageID field will cause validation to fail
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.ProcessHistoricalMessages(ctx, []model.UpsertMessagePayload{messagePayload}, nil)

	// Assert
	assert.Error(t, err, "Expected error due to validation failure")
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "validation error at index 0")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Validation failed for message" {
			foundError = true
			break
		}
	}
	assert.True(t, foundError, "Expected error log about validation failure")

	// Verify no repository calls or worker calls
	messageRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestProcessHistoricalMessagesClusterValidationError tests handling of cluster validation errors
func TestProcessHistoricalMessagesClusterValidationError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data - message with cluster that doesn't match tenant
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: "tenant-2",
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "tenant-1")

	// Call function
	err := service.ProcessHistoricalMessages(ctx, []model.UpsertMessagePayload{messagePayload}, nil)

	// Assert
	assert.Error(t, err, "Expected error due to cluster validation failure")
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "company validation error at index 0")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "CompanyID validation failed for message" {
			foundError = true
			assert.Equal(t, "msg-1", log.ContextMap()["message_id"])
			assert.Equal(t, "tenant-2", log.ContextMap()["company_id"])
			break
		}
	}
	assert.True(t, foundError, "Expected error log about cluster validation")

	// Verify no repository calls or worker calls
	messageRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestProcessHistoricalMessagesTenantError tests handling of errors when extracting tenant from context
func TestProcessHistoricalMessagesTenantError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data - valid payload but context without tenant
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: "tenant-1",
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	// Intentionally NOT adding tenant to context

	// Call function
	err := service.ProcessHistoricalMessages(ctx, []model.UpsertMessagePayload{messagePayload}, nil)

	// Assert
	assert.Error(t, err, "Expected error when tenant missing from context")
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "failed to get tenant ID from context")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Failed to get tenant ID from context" {
			foundError = true
			break
		}
	}
	assert.True(t, foundError, "Expected error log about missing tenant")

	// Verify repository calls or worker calls
	messageRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestProcessHistoricalMessagesEmptyID tests handling of messages with empty IDs
func TestProcessHistoricalMessagesEmptyID(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messagePayloads := []model.UpsertMessagePayload{
		{
			MessageID: "",
			CompanyID: companyID,
			Jid:       "jid-1",
			ChatID:    "chat-1",
			Flow:      "IN",
			Status:    "pending",
		},
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.ProcessHistoricalMessages(ctx, messagePayloads, nil)

	// Assert
	assert.Error(t, err)
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "validation error at index 0") // Validation happens first

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Validation failed for message" {
			foundError = true
			assert.Equal(t, "", log.ContextMap()["message_id"])
			break
		}
	}
	assert.True(t, foundError, "Expected error log about empty message ID")

	// No repository calls or worker calls should happen due to validation error
	messageRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestProcessHistoricalMessagesWithMetadata tests handling of messages with metadata
func TestProcessHistoricalMessagesWithMetadata(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
		FromPhone: "12345",
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 123,
		StreamSequence:   456,
		Stream:           "stream-1",
		Consumer:         "consumer-1",
		Domain:           "domain-1",
		MessageID:        "msg-id-1",
		MessageSubject:   "subject-1",
	}

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Setup expectations
	messageRepo.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Message")).Run(func(args mock.Arguments) {
		messages := args.Get(1).([]model.Message)
		assert.Equal(t, 1, len(messages))
		assert.Equal(t, "msg-1", messages[0].MessageID)
	}).Return(nil)

	// Expect onboarding task submission for the IN flow message
	onboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == "msg-1"
	})).Return(nil)

	// Call function
	err := service.ProcessHistoricalMessages(ctx, []model.UpsertMessagePayload{messagePayload}, metadata)

	// Assert
	assert.NoError(t, err)
	messageRepo.AssertExpectations(t)
	onboardingWorker.AssertExpectations(t)
}

// TestProcessHistoricalMessagesWithKey tests handling of messages with Key field
func TestProcessHistoricalMessagesWithKey(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t) // Need worker mock

	// Setup test data
	companyID := "tenant-1"
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
		FromPhone: "12345",
		Key: &model.KeyPayload{
			ID:        "key-1",
			FromMe:    true,
			RemoteJid: "remote-jid-1",
		},
	}

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Setup expectations
	messageRepo.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Message")).Run(func(args mock.Arguments) {
		messages := args.Get(1).([]model.Message)
		assert.Equal(t, 1, len(messages))
		assert.Equal(t, "msg-1", messages[0].MessageID)
	}).Return(nil)

	// Expect onboarding task submission for the IN flow message
	onboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == "msg-1"
	})).Return(nil)

	// Call function
	err := service.ProcessHistoricalMessages(ctx, []model.UpsertMessagePayload{messagePayload}, nil)

	// Assert
	assert.NoError(t, err)
	messageRepo.AssertExpectations(t)
	onboardingWorker.AssertExpectations(t) // Assert worker expectations
}

// TestProcessHistoricalMessagesRepositoryError tests handling of repository errors during bulk upsert
func TestProcessHistoricalMessagesRepositoryError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messagePayloads := []model.UpsertMessagePayload{
		{
			MessageID: "msg-1",
			Jid:       "jid-1",
			CompanyID: companyID,
			ChatID:    "chat-1",
			Flow:      "IN",
			Status:    "pending",
		},
	}

	// Mock repository error - simulate a database error
	dbError := errors.New("simulated DB error")
	wrappedDbError := fmt.Errorf("%w: %w", apperrors.ErrDatabase, dbError)
	messageRepo.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Message")).Return(wrappedDbError)

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.ProcessHistoricalMessages(ctx, messagePayloads, nil)

	// Assert
	assert.Error(t, err, "Expected error due to repository failure")

	// Check for RetryableError wrapper and underlying ErrDatabase
	var retryableErr *apperrors.RetryableError
	assert.True(t, errors.As(err, &retryableErr), "Expected error to be RetryableError")
	assert.True(t, errors.Is(err, apperrors.ErrDatabase), "Expected underlying error to be ErrDatabase")
	assert.Contains(t, err.Error(), "BulkUpsertHistoricalMessages failed: database error")
	assert.Contains(t, err.Error(), "simulated DB error") // Check original error is wrapped

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Repository operation failed: Database error" { // Check for log from handleRepositoryError
			foundError = true
			assert.Equal(t, "BulkUpsertHistoricalMessages", log.ContextMap()["operation"])
			break
		}
	}
	assert.True(t, foundError, "Expected error log from handleRepositoryError")

	// Verify repository call & no worker call due to early exit
	messageRepo.AssertCalled(t, "BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Message"))
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpdateMessageValidationError tests handling of validation errors
func TestUpdateMessageValidationError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data - incomplete payload to fail validation
	updatePayload := model.UpdateMessagePayload{
		// Missing ID field will cause validation to fail
		Status:    "delivered",
		CompanyID: "tenant-1",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "tenant-1")

	// Call function
	err := service.UpdateMessage(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err, "Expected error due to validation failure")
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "message update validation failed")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Message update validation failed" {
			foundError = true
			break
		}
	}
	assert.True(t, foundError, "Expected error log about validation failure")

	// Verify no repository calls or worker calls
	messageRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	messageRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpdateMessageTenantError tests handling of errors when extracting tenant from context
func TestUpdateMessageTenantError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	updatePayload := model.UpdateMessagePayload{
		MessageID: "msg-1",
		Status:    "delivered",
		CompanyID: "tenant-1",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	// Intentionally NOT adding tenant to context

	// Call function
	err := service.UpdateMessage(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err, "Expected error when tenant missing from context")
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "failed to get tenant ID from context")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Failed to get tenant ID from context" {
			foundError = true
			break
		}
	}
	assert.True(t, foundError, "Expected error log about missing tenant")

	// Verify no repository calls or worker calls
	messageRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	messageRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpdateMessageFindByIDError tests handling of errors during FindByMessageID
func TestUpdateMessageFindByIDError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messageID := "msg-find-fail"
	updatePayload := model.UpdateMessagePayload{
		MessageID: messageID,
		CompanyID: companyID,
		Status:    "updated",
	}

	// Mock repository error for FindByMessageID - simulate DB error
	dbError := errors.New("simulated find error")
	wrappedDbError := fmt.Errorf("%w: %w", apperrors.ErrDatabase, dbError)
	messageRepo.On("FindByMessageID", mock.Anything, messageID).Return(nil, wrappedDbError)

	// Create observable logger
	observedZapCore, _ := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.UpdateMessage(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err, "Expected error due to FindByMessageID failure")

	// Check for RetryableError wrapper and underlying ErrDatabase
	var retryableErr *apperrors.RetryableError
	assert.True(t, errors.As(err, &retryableErr), "Expected error to be RetryableError")
	assert.True(t, errors.Is(err, apperrors.ErrDatabase), "Expected underlying error to be ErrDatabase")
	assert.Contains(t, err.Error(), "FindMessageByIDForUpdate failed: database error")
	assert.Contains(t, err.Error(), "simulated find error") // Check original error is wrapped

	// Verify calls
	messageRepo.AssertCalled(t, "FindByMessageID", mock.Anything, messageID)
	messageRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpdateMessageMessageNotFound tests handling when the message to update is not found
func TestUpdateMessageMessageNotFound(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messageID := "msg-not-found"
	updatePayload := model.UpdateMessagePayload{
		MessageID: messageID,
		CompanyID: companyID,
		Status:    "updated",
	}

	// Mock repository error for FindByMessageID - simulate not found
	notFoundError := errors.New("gorm: record not found") // Simulate GORM error
	wrappedNotFoundError := fmt.Errorf("%w: %w", apperrors.ErrNotFound, notFoundError)
	messageRepo.On("FindByMessageID", mock.Anything, messageID).Return(nil, wrappedNotFoundError)

	// Create observable logger
	observedZapCore, _ := observer.New(zap.WarnLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.UpdateMessage(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err, "Expected error because message was not found")

	// Check for FatalError wrapper and underlying ErrNotFound
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.True(t, errors.Is(err, apperrors.ErrNotFound), "Expected underlying error to be ErrNotFound")
	assert.Contains(t, err.Error(), "FindMessageByIDForUpdate failed: resource not found")
	assert.Contains(t, err.Error(), "gorm: record not found") // Check original error is wrapped

	// Verify calls
	messageRepo.AssertCalled(t, "FindByMessageID", mock.Anything, messageID)
	messageRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpdateMessageTenantMismatch tests handling of tenant mismatch during update
func TestUpdateMessageTenantMismatch(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	contextTenantID := "tenant-1"
	messageTenantID := "tenant-2" // Different from context tenant ID
	updatePayload := model.UpdateMessagePayload{
		MessageID: "msg-1",
		Status:    "delivered",
		CompanyID: contextTenantID,
	}

	// Create message with mismatched tenant ID
	existingMessage := &model.Message{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: messageTenantID, // Different tenant ID
		CreatedAt: time.Now().Add(-24 * time.Hour),
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.WarnLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, contextTenantID)

	// Setup expectations
	messageRepo.On("FindByMessageID", ctx, "msg-1").Return(existingMessage, nil)

	// Call function
	err := service.UpdateMessage(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err, "Expected error due to tenant ID mismatch")
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "company ID mismatch for message") // Check both payload/context vs existing mismatch

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundWarning bool
	for _, log := range observedLogs.All() {
		// Correct the expected log message string here
		if log.Message == "Company ID mismatch during update (context vs existing)" || log.Message == "Company ID mismatch during update (payload vs existing)" {
			foundWarning = true
			assert.Equal(t, messageTenantID, log.ContextMap()["existing_company_id"])
			// The specific mismatch logged depends on which check fails first, so accept either
			if log.Message == "Company ID mismatch during update (payload vs existing)" {
				assert.Equal(t, contextTenantID, log.ContextMap()["payload_company_id"])
			} else {
				assert.Equal(t, contextTenantID, log.ContextMap()["context_company_id"])
			}
			break
		}
	}
	assert.True(t, foundWarning, "Expected warning log about tenant ID mismatch")

	// Verify repository calls
	messageRepo.AssertCalled(t, "FindByMessageID", ctx, "msg-1")
	messageRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpdateMessageUpdateError tests handling of errors during the actual Update call
func TestUpdateMessageUpdateError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messageID := "msg-update-fail"
	updatePayload := model.UpdateMessagePayload{
		MessageID: messageID,
		CompanyID: companyID,
		Status:    "new-status",
	}

	// Mock successful FindByMessageID
	existingMessage := &model.Message{
		MessageID: messageID,
		CompanyID: companyID,
		Status:    "old-status",
	}
	messageRepo.On("FindByMessageID", mock.Anything, messageID).Return(existingMessage, nil)

	// Mock repository error for Update - simulate a DB error
	dbError := errors.New("simulated update error")
	wrappedDbError := fmt.Errorf("%w: %w", apperrors.ErrDatabase, dbError)
	messageRepo.On("Update", mock.Anything, mock.MatchedBy(func(msg model.Message) bool {
		return msg.MessageID == messageID && msg.Status == "new-status"
	})).Return(wrappedDbError)

	// Create observable logger
	observedZapCore, _ := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.UpdateMessage(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err, "Expected error due to Update failure")

	// Check for RetryableError wrapper and underlying ErrDatabase
	var retryableErr *apperrors.RetryableError
	assert.True(t, errors.As(err, &retryableErr), "Expected error to be RetryableError")
	assert.True(t, errors.Is(err, apperrors.ErrDatabase), "Expected underlying error to be ErrDatabase")
	assert.Contains(t, err.Error(), "UpdateMessage failed: database error")
	assert.Contains(t, err.Error(), "simulated update error") // Check original error is wrapped

	// Verify calls
	messageRepo.AssertCalled(t, "FindByMessageID", mock.Anything, messageID)
	messageRepo.AssertCalled(t, "Update", mock.Anything, mock.MatchedBy(func(msg model.Message) bool {
		return msg.MessageID == messageID && msg.Status == "new-status"
	}))
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestProcessHistoricalMessagesWithClusterMismatch tests historical messages with cluster mismatch
func TestProcessHistoricalMessagesWithClusterMismatch(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messagePayloads := []model.UpsertMessagePayload{
		{
			MessageID: "msg-1",
			CompanyID: "different-tenant",
			Jid:       "jid-1",
			ChatID:    "chat-1",
			Flow:      "IN",
			Status:    "pending",
		},
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.WarnLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.ProcessHistoricalMessages(ctx, messagePayloads, nil)

	// Assert
	assert.Error(t, err)
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "company validation error at index 0")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundWarning bool
	for _, log := range observedLogs.All() {
		if strings.Contains(log.Message, "CompanyID validation failed") {
			foundWarning = true
			assert.Equal(t, "msg-1", log.ContextMap()["message_id"])
			assert.Equal(t, "different-tenant", log.ContextMap()["company_id"])
			assert.Equal(t, companyID, log.ContextMap()["context_company_id"])
			break
		}
	}
	assert.True(t, foundWarning, "Expected warning log about cluster validation failure")

	// No repository calls or worker calls should happen due to validation error
	messageRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpsertMessageValidationError tests handling of validation errors during upsert
func TestUpsertMessageValidationError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup invalid test data (missing required MessageID field)
	messagePayload := model.UpsertMessagePayload{
		Jid:       "jid-1",
		CompanyID: "tenant-1",
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "tenant-1")

	// Call function
	err := service.UpsertMessage(ctx, messagePayload, nil)

	// Assert
	assert.Error(t, err)
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "message validation failed")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Message validation failed" {
			foundError = true
			assert.Contains(t, log.ContextMap()["error"].(string), "message_id")
			break
		}
	}
	assert.True(t, foundError, "Expected error log about validation failure")

	// Verify no repository calls or worker calls were made
	messageRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpsertMessageClusterValidationError tests handling of cluster validation errors during upsert
func TestUpsertMessageClusterValidationError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data with mismatched cluster
	companyID := "tenant-1"
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: "different-tenant",
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.UpsertMessage(ctx, messagePayload, nil)

	// Assert
	assert.Error(t, err)
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "company_id validation error")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "CompanyID validation failed for message" {
			foundError = true
			assert.Equal(t, "msg-1", log.ContextMap()["message_id"])
			assert.Equal(t, "different-tenant", log.ContextMap()["company_id"])
			break
		}
	}
	assert.True(t, foundError, "Expected error log about cluster validation")

	// Verify no repository calls or worker calls
	messageRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	onboardingWorker.AssertNotCalled(t, "SubmitTask", mock.Anything)
}

// TestUpsertMessageTenantError tests handling of errors when extracting tenant from context during upsert
func TestUpsertMessageTenantError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, _ := setupTestServiceWithMocks(t) // Discard unused worker

	// Setup test data
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: "tenant-1",
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	// Intentionally NOT adding tenant to context

	// Call function
	err := service.UpsertMessage(ctx, messagePayload, nil)

	// Assert
	assert.Error(t, err, "Expected error when tenant missing from context")
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.Contains(t, err.Error(), "failed to get tenant ID from context")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		if log.Message == "Failed to get tenant ID from context" {
			foundError = true
			break
		}
	}
	assert.True(t, foundError, "Expected error log about missing tenant")

	// Verify no repository calls
	messageRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

// TestUpsertMessageWithMetadata tests successful message upsert with metadata
func TestUpsertMessageWithMetadata(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
		FromPhone: "12345",
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 123,
		StreamSequence:   456,
		Stream:           "stream-1",
		Consumer:         "consumer-1",
		Domain:           "domain-1",
		MessageID:        "msg-id-1",
		MessageSubject:   "subject-1",
	}

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Setup expectations
	messageRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Message")).Run(func(args mock.Arguments) {
		message := args.Get(1).(model.Message)
		assert.Equal(t, "msg-1", message.MessageID)
	}).Return(nil)
	onboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == "msg-1"
	})).Return(nil)

	// Call function
	err := service.UpsertMessage(ctx, messagePayload, metadata)

	// Assert
	assert.NoError(t, err)
	messageRepo.AssertExpectations(t)
}

// TestUpsertMessageWithKey tests successful message upsert with key data
func TestUpsertMessageWithKey(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t) // Need worker mock

	// Setup test data
	companyID := "tenant-1"
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-1",
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
		FromPhone: "12345",
		Key: &model.KeyPayload{
			ID:        "key-1",
			FromMe:    true,
			RemoteJid: "remote-jid-1",
		},
	}

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Setup expectations
	messageRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Message")).Run(func(args mock.Arguments) {
		message := args.Get(1).(model.Message)
		assert.Equal(t, "msg-1", message.MessageID)
	}).Return(nil)

	// Expect onboarding task submission
	onboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == "msg-1"
	})).Return(nil)

	// Call function
	err := service.UpsertMessage(ctx, messagePayload, nil)

	// Assert
	assert.NoError(t, err)
	messageRepo.AssertExpectations(t)
	onboardingWorker.AssertExpectations(t) // Assert worker expectations
}

// TestUpsertMessageKeyValidationError tests handling of key validation errors during upsert
func TestUpsertMessageKeyValidationError(t *testing.T) {
	// Setup
	service, _, messageRepo, _, _, _, _, _ := setupTestServiceWithMocks(t) // Discard unused worker

	// Setup test data: VALID main payload, INVALID Key payload
	companyID := "tenant-1"
	messagePayload := model.UpsertMessagePayload{
		MessageID: "msg-valid-payload-invalid-key", // Ensure main payload is valid
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
		Key: &model.KeyPayload{
			// Missing required ID field makes the Key invalid
			FromMe:    true,
			RemoteJid: "remote-jid-1",
		},
	}

	// Create observable logger to capture logs
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.UpsertMessage(ctx, messagePayload, nil)

	// Assert
	assert.Error(t, err)
	// Check for FatalError wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	// Assert the error message now correctly reflects the key validation failure
	assert.Contains(t, err.Error(), "message validation failed")
	// Check that the underlying validation error message is present
	assert.Contains(t, err.Error(), "field 'id' failed validation: is required")

	// Verify logs
	assert.GreaterOrEqual(t, observedLogs.Len(), 1, "Expected at least one log message")
	var foundError bool
	for _, log := range observedLogs.All() {
		// Expect the log message specific to key validation failure
		if log.Message == "Message validation failed" {
			foundError = true
			assert.Equal(t, messagePayload.MessageID, log.ContextMap()["message_id"])
			assert.Contains(t, log.ContextMap()["error"].(string), "field 'id' failed validation: is required")
			break
		}
	}
	assert.True(t, foundError, "Expected error log about key validation failure")

	// Verify repository calls
	messageRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

// TestUpsertMessageRepositoryError tests handling of repository errors during upsert
func TestUpsertMessageRepositoryError(t *testing.T) {
	// Setup
	service, _, messageRepo, contactRepo, _, onboardingRepo, _, _ := setupTestServiceWithMocks(t) // Discard unused worker

	// Setup test data
	companyID := "tenant-1"
	messageID := "msg-save-fail"
	upsertPayload := model.UpsertMessagePayload{
		MessageID: messageID,
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		Status:    "pending",
	}

	// Mock repository error for Save - simulate a duplicate error
	dupError := errors.New("duplicate key violation")
	wrappedDupError := fmt.Errorf("%w: %w", apperrors.ErrDuplicate, dupError)
	messageRepo.On("Save", mock.Anything, mock.MatchedBy(func(msg model.Message) bool {
		return msg.MessageID == messageID
	})).Return(wrappedDupError)

	// Create observable logger
	observedZapCore, _ := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.UpsertMessage(ctx, upsertPayload, nil)

	// Assert
	assert.Error(t, err, "Expected error due to Save failure")

	// Check for FatalError wrapper and underlying ErrDuplicate
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected error to be FatalError")
	assert.True(t, errors.Is(err, apperrors.ErrDuplicate), "Expected underlying error to be ErrDuplicate")
	assert.Contains(t, err.Error(), "SaveMessage failed: duplicate resource")
	assert.Contains(t, err.Error(), "duplicate key violation") // Check original error is wrapped

	// Verify calls
	messageRepo.AssertCalled(t, "Save", mock.Anything, mock.MatchedBy(func(msg model.Message) bool {
		return msg.MessageID == messageID
	}))
	// Ensure onboarding log creation wasn't attempted due to early exit
	contactRepo.AssertNotCalled(t, "FindByPhone", mock.Anything, mock.Anything)
	onboardingRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	onboardingRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

// TestUpsertMessageCreatesOnboardingLog tests that an onboarding log is created for a new contact
func TestUpsertMessageCreatesOnboardingLog(t *testing.T) {
	// Setup
	// Use specific mock names returned by the helper
	service, _, mockMessageRepo, _, _, _, _, mockOnboardingWorker := setupTestServiceWithMocks(t)

	companyID := "onboard-tenant-1"
	messageID := "onboard-msg-1"

	payload := model.UpsertMessagePayload{
		MessageID:        messageID,
		Jid:              "jid-onboard",
		CompanyID:        companyID,
		ChatID:           "chat-onboard-1",
		Status:           "received",
		FromPhone:        "+1 (234) 567-890",
		Flow:             "IN",
		MessageTimestamp: time.Now().Unix(),
	}

	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Expectations
	mockMessageRepo.On("Save", mock.Anything, mock.MatchedBy(func(m model.Message) bool {
		return m.MessageID == messageID
	})).Return(nil)
	// ContactRepo and OnboardingLogRepo checks are now inside the worker, so we only check submission
	// Re-add the expectation for SubmitTask
	mockOnboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == messageID
	})).Return(nil)

	// Call function
	err := service.UpsertMessage(ctx, payload, nil)

	// Assert
	assert.NoError(t, err)
	mockMessageRepo.AssertExpectations(t)
	// Remove assertions for repos called inside the worker
	// mockContactRepo.AssertExpectations(t)
	// mockOnboardingLogRepo.AssertExpectations(t)
	mockOnboardingWorker.AssertExpectations(t)
}

// TestUpsertMessageSkipsOnboardingLogForExistingContact tests that log is not created if contact exists
func TestUpsertMessageSkipsOnboardingLogForExistingContact(t *testing.T) {
	// Setup
	// Use specific mock names returned by the helper
	service, _, mockMessageRepo, _, _, _, _, mockOnboardingWorker := setupTestServiceWithMocks(t)

	companyID := "onboard-tenant-2"
	messageID := "onboard-msg-2"

	payload := model.UpsertMessagePayload{
		MessageID:        messageID,
		Jid:              "jid-onboard-2",
		CompanyID:        companyID,
		ChatID:           "chat-onboard-2",
		Status:           "read",
		FromPhone:        "9876543210", // Keep the number in the payload
		Flow:             "IN",
		MessageTimestamp: time.Now().Unix(),
	}

	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Message should be saved
	mockMessageRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Message")).Return(nil)
	// Onboarding task should still be SUBMITTED. The worker will handle the check.
	mockOnboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == messageID
	})).Return(nil)

	// Call function
	err := service.UpsertMessage(ctx, payload, nil)

	// Assert
	assert.NoError(t, err)
	mockMessageRepo.AssertCalled(t, "Save", mock.Anything, mock.Anything)
	// Remove contact repo assertion
	// mockContactRepo.AssertCalled(t, "FindByPhone", mock.Anything, phoneNum)
	mockOnboardingWorker.AssertCalled(t, "SubmitTask", mock.Anything)
	// Remove assertions for repos called inside the worker
	// mockOnboardingLogRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	// mockOnboardingLogRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

// TestUpsertMessageSkipsOnboardingLogForExistingLog tests that log is not created if it already exists for message ID
func TestUpsertMessageSkipsOnboardingLogForExistingLog(t *testing.T) {
	// Setup
	service, _, mockMessageRepo, mockContactRepo, _, mockOnboardingLogRepo, _, mockOnboardingWorker := setupTestServiceWithMocks(t)

	companyID := "onboard-tenant-3"
	messageID := "onboard-msg-3"

	payload := model.UpsertMessagePayload{
		MessageID:        messageID,
		Jid:              "jid-onboard-3",
		CompanyID:        companyID,
		ChatID:           "chat-onboard-3",
		Status:           "delivered",
		FromPhone:        "1122334455",
		Flow:             "IN",
		MessageTimestamp: time.Now().Unix(),
	}

	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Expectations
	mockOnboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == "onboard-msg-3"
	})).Return(nil)
	mockMessageRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Message")).Return(nil)

	// Call function
	err := service.UpsertMessage(ctx, payload, nil)

	// Assert
	assert.NoError(t, err)
	mockMessageRepo.AssertCalled(t, "Save", mock.Anything, mock.Anything)
	mockContactRepo.AssertNotCalled(t, "FindByPhone", mock.Anything, "1122334455")
	mockOnboardingLogRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, messageID)
	mockOnboardingLogRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	mockOnboardingWorker.AssertCalled(t, "SubmitTask", mock.Anything)
}

// TestUpsertMessageSkipsOnboardingLogForOutgoingMessage tests that log is not created for non-IN flow
func TestUpsertMessageSkipsOnboardingLogForOutgoingMessage(t *testing.T) {
	// Setup
	service, _, mockMessageRepo, mockContactRepo, _, mockOnboardingLogRepo, _, _ := setupTestServiceWithMocks(t)

	companyID := "onboard-tenant-4"
	messageID := "onboard-msg-4"

	payload := model.UpsertMessagePayload{
		MessageID:        messageID,
		Jid:              "jid-onboard-4",
		CompanyID:        companyID,
		ChatID:           "chat-onboard-4",
		Status:           "sent",
		FromPhone:        "agent-sender",
		ToPhone:          "1234567890",
		Flow:             "OUT",
		MessageTimestamp: time.Now().Unix(),
	}

	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Expectations
	mockMessageRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Message")).Return(nil)

	// Call function
	err := service.UpsertMessage(ctx, payload, nil)

	// Assert
	assert.NoError(t, err)
	mockMessageRepo.AssertCalled(t, "Save", mock.Anything, mock.Anything)
	mockContactRepo.AssertNotCalled(t, "FindByPhone", mock.Anything, mock.Anything)
	mockOnboardingLogRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	mockOnboardingLogRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

// TestUpsertMessageHandlesLogRepoFindError tests handling of errors when checking for existing log
func TestUpsertMessageHandlesLogRepoFindError(t *testing.T) {
	// Setup - Note: This test becomes less relevant as the check moves to the worker.
	// We now test if the task is submitted regardless of potential downstream worker errors.
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messageID := "msg-onboard-log-find-fail"
	phoneNumber := "1234567890"
	upsertPayload := model.UpsertMessagePayload{
		MessageID: messageID,
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		FromPhone: phoneNumber,
		Status:    "pending",
	}

	// Mock successful message save
	messageRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Message")).Return(nil)

	// Expect task to be submitted even if log check fails later in worker
	onboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == messageID
	})).Return(nil)

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.WarnLevel) // Expecting Warning
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.UpsertMessage(ctx, upsertPayload, nil)

	// Assert
	assert.NoError(t, err, "Expected no error from UpsertMessage itself")

	// Verify logs - No warning expected here now, worker handles logging
	assert.Equal(t, 0, observedLogs.Len())

	// Verify calls
	messageRepo.AssertCalled(t, "Save", mock.Anything, mock.AnythingOfType("model.Message"))
	// Remove assertions for repo calls - happen in worker
	// contactRepo.AssertCalled(t, "FindByPhone", mock.Anything, phoneNumber)
	// onboardingRepo.AssertCalled(t, "FindByMessageID", mock.Anything, messageID)
	onboardingWorker.AssertCalled(t, "SubmitTask", mock.Anything)
	// onboardingRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

// TestUpsertMessageHandlesLogRepoSaveError tests handling of errors when saving a new log
func TestUpsertMessageHandlesLogRepoSaveError(t *testing.T) {
	// Setup - Note: This test becomes less relevant as the check moves to the worker.
	// We now test if the task is submitted regardless of potential downstream worker errors.
	service, _, messageRepo, _, _, _, _, onboardingWorker := setupTestServiceWithMocks(t)

	// Setup test data
	companyID := "tenant-1"
	messageID := "msg-onboard-log-save-fail"
	phoneNumber := "1234567890"
	upsertPayload := model.UpsertMessagePayload{
		MessageID: messageID,
		Jid:       "jid-1",
		CompanyID: companyID,
		ChatID:    "chat-1",
		Flow:      "IN",
		FromPhone: phoneNumber,
		Status:    "pending",
	}

	// Mock successful message save
	messageRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Message")).Return(nil)

	// Expect task to be submitted even if log save fails later in worker
	onboardingWorker.On("SubmitTask", mock.MatchedBy(func(data OnboardingTaskData) bool {
		return data.Message.MessageID == messageID
	})).Return(nil)

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.WarnLevel) // Expecting Warning
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.UpsertMessage(ctx, upsertPayload, nil)

	// Assert
	assert.NoError(t, err, "Expected no error from UpsertMessage itself")

	// Verify logs - No warning expected here now, worker handles logging
	assert.Equal(t, 0, observedLogs.Len())

	// Verify calls
	messageRepo.AssertCalled(t, "Save", mock.Anything, mock.AnythingOfType("model.Message"))
	// Remove assertions for repo calls - happen in worker
	// contactRepo.AssertCalled(t, "FindByPhone", mock.Anything, phoneNumber)
	// onboardingRepo.AssertCalled(t, "FindByMessageID", mock.Anything, messageID)
	onboardingWorker.AssertCalled(t, "SubmitTask", mock.Anything)
	// onboardingRepo.AssertCalled(t, "Save", mock.Anything, mock.MatchedBy(func(log model.OnboardingLog) bool {
	// 	return log.MessageID == messageID && log.PhoneNumber == phoneNumber
	// }))
}

// --- Test for handleRepositoryError ---

func TestHandleRepositoryError(t *testing.T) {
	// Setup observable logger to potentially check logs if needed
	observedZapCore, _ := observer.New(zap.InfoLevel)
	observedLogger := zap.New(observedZapCore)
	// Create a context with the test logger
	ctx := logger.WithLogger(context.Background(), observedLogger)

	testCases := []struct {
		name            string
		inputErr        error
		operation       string
		messageID       string
		expectNil       bool
		expectFatal     bool
		expectRetryable bool
		expectIs        error // Expected underlying standard error (if any)
		containsMsg     string
	}{
		{
			name:            "Nil Error",
			inputErr:        nil,
			operation:       "TestOp",
			messageID:       "msg-nil",
			expectNil:       true,
			expectFatal:     false,
			expectRetryable: false,
			expectIs:        nil,
			containsMsg:     "",
		},
		{
			name:            "Not Found Error",
			inputErr:        fmt.Errorf("%w: item not located", apperrors.ErrNotFound),
			operation:       "FindItem",
			messageID:       "msg-notfound",
			expectNil:       false,
			expectFatal:     true,
			expectRetryable: false,
			expectIs:        apperrors.ErrNotFound,
			containsMsg:     "FindItem failed: resource not found",
		},
		{
			name:            "Duplicate Error",
			inputErr:        fmt.Errorf("%w: already exists", apperrors.ErrDuplicate),
			operation:       "CreateItem",
			messageID:       "msg-dup",
			expectNil:       false,
			expectFatal:     true,
			expectRetryable: false,
			expectIs:        apperrors.ErrDuplicate,
			containsMsg:     "CreateItem failed: duplicate resource",
		},
		{
			name:            "Bad Request Error",
			inputErr:        fmt.Errorf("%w: invalid data", apperrors.ErrBadRequest),
			operation:       "UpdateItem",
			messageID:       "msg-badreq",
			expectNil:       false,
			expectFatal:     true,
			expectRetryable: false,
			expectIs:        apperrors.ErrBadRequest,
			containsMsg:     "UpdateItem failed: bad request data",
		},
		{
			name:            "Database Error",
			inputErr:        fmt.Errorf("%w: connection lost", apperrors.ErrDatabase),
			operation:       "QueryItems",
			messageID:       "msg-db",
			expectNil:       false,
			expectFatal:     false,
			expectRetryable: true,
			expectIs:        apperrors.ErrDatabase,
			containsMsg:     "QueryItems failed: database error",
		},
		{
			name:            "Timeout Error",
			inputErr:        fmt.Errorf("%w: deadline exceeded", apperrors.ErrTimeout),
			operation:       "LongOp",
			messageID:       "msg-timeout",
			expectNil:       false,
			expectFatal:     false,
			expectRetryable: true,
			expectIs:        apperrors.ErrTimeout,
			containsMsg:     "LongOp failed: operation timeout",
		},
		{
			name:            "NATS Error",
			inputErr:        fmt.Errorf("%w: publish failed", apperrors.ErrNATS),
			operation:       "PublishEvent",
			messageID:       "msg-nats",
			expectNil:       false,
			expectFatal:     false,
			expectRetryable: true,
			expectIs:        apperrors.ErrNATS,
			containsMsg:     "PublishEvent failed: NATS communication error",
		},
		{
			name:            "Unauthorized Error",
			inputErr:        fmt.Errorf("%w: invalid token", apperrors.ErrUnauthorized),
			operation:       "AuthCheck",
			messageID:       "msg-auth",
			expectNil:       false,
			expectFatal:     true,
			expectRetryable: false,
			expectIs:        apperrors.ErrUnauthorized,
			containsMsg:     "AuthCheck failed: unauthorized",
		},
		{
			name:            "Conflict Error",
			inputErr:        fmt.Errorf("%w: version mismatch", apperrors.ErrConflict),
			operation:       "OptimisticUpdate",
			messageID:       "msg-conflict",
			expectNil:       false,
			expectFatal:     true,
			expectRetryable: false,
			expectIs:        apperrors.ErrConflict,
			containsMsg:     "OptimisticUpdate failed: resource conflict",
		},
		{
			name:            "Generic Error",
			inputErr:        errors.New("something unexpected happened"),
			operation:       "GenericOp",
			messageID:       "msg-generic",
			expectNil:       false,
			expectFatal:     true,
			expectRetryable: false,
			expectIs:        nil, // No specific standard error to check for
			containsMsg:     "GenericOp failed: unexpected repository error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Pass the context to the function call
			returnedErr := handleRepositoryError(ctx, tc.inputErr, tc.operation, tc.messageID)

			if tc.expectNil {
				assert.NoError(t, returnedErr)
				return
			}

			assert.Error(t, returnedErr)

			if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(returnedErr, &fatalErr), "Expected FatalError, got %T", returnedErr)
				assert.False(t, errors.As(returnedErr, new(*apperrors.RetryableError)), "Did not expect RetryableError")
				assert.Contains(t, returnedErr.Error(), tc.containsMsg)
				// Check if the original error is wrapped correctly
				assert.True(t, errors.Is(returnedErr, tc.inputErr), "FatalError should wrap the original error")
			}

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(returnedErr, &retryableErr), "Expected RetryableError, got %T", returnedErr)
				assert.False(t, errors.As(returnedErr, new(*apperrors.FatalError)), "Did not expect FatalError")
				assert.Contains(t, returnedErr.Error(), tc.containsMsg)
				// Check if the original error is wrapped correctly
				assert.True(t, errors.Is(returnedErr, tc.inputErr), "RetryableError should wrap the original error")
			}

			if tc.expectIs != nil {
				assert.True(t, errors.Is(returnedErr, tc.expectIs), "Expected underlying error to be %T", tc.expectIs)
			}
		})
	}
}
