package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"gorm.io/datatypes"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	storagemock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

// setupOnboardingWorkerTest creates mocks and the worker instance for testing.
// Note: We don't initialize the actual ants pool for unit testing processOnboardingTask.
func setupOnboardingWorkerTest(t *testing.T) (*OnboardingWorker, *storagemock.ContactRepoMock, *storagemock.OnboardingLogRepoMock, *observer.ObservedLogs) {
	contactRepo := new(storagemock.ContactRepoMock)
	logRepo := new(storagemock.OnboardingLogRepoMock)

	// Setup observed logger
	observedZapCore, observedLogs := observer.New(zapcore.DebugLevel) // Capture debug logs
	testLogger := zap.New(observedZapCore).Named("test_onboarding_worker")

	// Create worker instance without initializing the pool
	worker := &OnboardingWorker{
		contactRepo: contactRepo,
		logRepo:     logRepo,
		// cfg is not used directly by processOnboardingTask
		baseLogger: testLogger,
	}

	return worker, contactRepo, logRepo, observedLogs
}

// createTestOnboardingTaskData creates default task data for tests.
// Includes a default AgentID now.
func createTestOnboardingTaskData(ctx context.Context, companyID, messageID, from, agentID string) OnboardingTaskData {
	if agentID == "" {
		agentID = "default-test-agent-" + uuid.New().String()[:4] // Ensure agentID is not empty
	}
	return OnboardingTaskData{
		Ctx: ctx,
		Message: model.Message{
			MessageID:        messageID,
			FromPhone:        from,
			CompanyID:        companyID,
			AgentID:          agentID, // Included agent ID
			Flow:             "IN",    // Default to IN flow for most tests
			MessageTimestamp: time.Now().Unix(),
			Jid:              "test-jid",
			ChatID:           "test-chat",
			Status:           "received",
		},
		MetadataJSON: datatypes.JSON(`{"test":"meta"}`),
	}
}

func TestProcessOnboardingTask_HappyPath(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-happy"
	messageID := "msg-happy"
	phoneNum := "1234567890"
	cleanedPhone := "1234567890"
	agentID := "agent-happy"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger) // Inject logger into context

	// Expectations: Contact not found by phone+agent, Contact Save success, Log not found, Log Save success
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, cleanedPhone, agentID).Return(nil, apperrors.ErrNotFound).Once()
	contactRepo.On("Save", mock.Anything, mock.MatchedBy(func(c model.Contact) bool {
		return c.PhoneNumber == cleanedPhone && c.AgentID == agentID && c.CompanyID == companyID && c.Origin == "onboarding_worker"
	})).Return(nil).Once() // Expect contact save
	logRepo.On("FindByMessageID", mock.Anything, messageID).Return(nil, apperrors.ErrNotFound).Once()
	logRepo.On("Save", mock.Anything, mock.MatchedBy(func(log model.OnboardingLog) bool {
		return log.MessageID == messageID && log.PhoneNumber == cleanedPhone && log.CompanyID == companyID && log.AgentID == agentID
	})).Return(nil).Once()

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertExpectations(t)
	logRepo.AssertExpectations(t)
	assert.GreaterOrEqual(t, observedLogs.Len(), 2, "Expected logs for contact and log creation")
	assert.True(t, observedLogs.FilterMessage("Successfully created new contact during onboarding").Len() == 1, "Expected contact creation log")
	assert.True(t, observedLogs.FilterMessage("Successfully created onboarding log").Len() == 1, "Expected log creation log")
}

func TestProcessOnboardingTask_ExistingContact(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-existing-contact"
	messageID := "msg-existing-contact"
	phoneNum := "+1 (800) 555-1212"
	cleanedPhone := "18005551212"
	agentID := "agent-existing"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Expectation: Contact IS found by phone+agent
	existingContact := &model.Contact{PhoneNumber: cleanedPhone, AgentID: agentID, CompanyID: companyID}
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, cleanedPhone, agentID).Return(existingContact, nil).Once()

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertExpectations(t)
	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything) // No save called
	logRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	assert.True(t, observedLogs.FilterMessage("Skipping onboarding: Contact already exists for this phone and agent combo").Len() == 1, "Expected skip log")
}

func TestProcessOnboardingTask_ExistingLog(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-existing-log"
	messageID := "msg-existing-log"
	phoneNum := "555-123-4567"
	cleanedPhone := "5551234567"
	agentID := "agent-log-exists"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Expectations: Contact not found by phone+agent, Contact save success, Log IS found
	existingLog := &model.OnboardingLog{MessageID: messageID}
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, cleanedPhone, agentID).Return(nil, apperrors.ErrNotFound).Once()
	contactRepo.On("Save", mock.Anything, mock.MatchedBy(func(c model.Contact) bool { // Contact save is still expected
		return c.PhoneNumber == cleanedPhone && c.AgentID == agentID && c.CompanyID == companyID
	})).Return(nil).Once()
	logRepo.On("FindByMessageID", mock.Anything, messageID).Return(existingLog, nil).Once()

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertExpectations(t)
	logRepo.AssertExpectations(t)
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything) // Log save not called
	assert.True(t, observedLogs.FilterMessage("Onboarding log already exists for this message, contact was created").Len() == 1, "Expected skip log")
}

func TestProcessOnboardingTask_ErrorCheckingContact(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-contact-err"
	messageID := "msg-contact-err"
	phoneNum := "1112223333"
	cleanedPhone := "1112223333"
	agentID := "agent-contact-err"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Expectation: Contact check returns a DB error (not NotFound or BadRequest)
	dbError := errors.New("simulated contact check db error")
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, cleanedPhone, agentID).Return(nil, fmt.Errorf("%w: %w", apperrors.ErrDatabase, dbError)).Once()

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertExpectations(t)
	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	assert.True(t, observedLogs.FilterMessage("Error checking contact existence by phone and agent").Len() == 1, "Expected error log")
}

func TestProcessOnboardingTask_ErrorCheckingContact_BadRequest(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-contact-badreq"
	messageID := "msg-contact-badreq"
	phoneNum := "1112223333"
	cleanedPhone := "1112223333"
	agentID := "" // Empty agent ID to trigger bad request in mock potentially

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	taskData.Message.AgentID = "" // Explicitly ensure AgentID is empty for this test case, overriding helper default if needed
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Expectation: Contact check returns BadRequest (e.g., due to empty agent ID)
	badReqError := fmt.Errorf("%w: agentID cannot be empty", apperrors.ErrBadRequest)
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, cleanedPhone, "").Return(nil, badReqError).Once() // Expect empty string for agentID

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertExpectations(t)
	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	assert.True(t, observedLogs.FilterMessage("Skipping onboarding task: Invalid input for contact check (e.g., empty AgentID)").Len() >= 1, "Expected warn log for bad request")
}

func TestProcessOnboardingTask_ErrorSavingContact(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-contact-save-err"
	messageID := "msg-contact-save-err"
	phoneNum := "2223334444"
	cleanedPhone := "2223334444"
	agentID := "agent-contact-save-err"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Expectations: Contact not found, Contact Save fails
	dbError := errors.New("simulated contact save db error")
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, cleanedPhone, agentID).Return(nil, apperrors.ErrNotFound).Once()
	contactRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Contact")).Return(fmt.Errorf("%w: %w", apperrors.ErrDatabase, dbError)).Once()

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertExpectations(t)
	logRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything) // Should not proceed to log check
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	assert.True(t, observedLogs.FilterMessage("Error saving new contact during onboarding").Len() == 1, "Expected contact save error log")
}

func TestProcessOnboardingTask_ErrorCheckingLog(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-log-check-err"
	messageID := "msg-log-check-err"
	phoneNum := "4445556666"
	cleanedPhone := "4445556666"
	agentID := "agent-log-check-err"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Expectations: Contact not found, Contact save success, Log check returns DB error
	dbError := errors.New("simulated log check db error")
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, cleanedPhone, agentID).Return(nil, apperrors.ErrNotFound).Once()
	contactRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Contact")).Return(nil).Once() // Contact save succeeds
	logRepo.On("FindByMessageID", mock.Anything, messageID).Return(nil, fmt.Errorf("%w: %w", apperrors.ErrDatabase, dbError)).Once()

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertExpectations(t)
	logRepo.AssertExpectations(t)
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything) // Log save not called
	assert.True(t, observedLogs.FilterMessage("Error checking for existing onboarding log after contact creation").Len() == 1, "Expected error log")
}

func TestProcessOnboardingTask_ErrorSavingLog(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-log-save-err"
	messageID := "msg-log-save-err"
	phoneNum := "7778889999"
	cleanedPhone := "7778889999"
	agentID := "agent-log-save-err"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Expectations: Contact not found, Contact Save success, Log not found, Log Save returns error
	dbError := errors.New("simulated log save db error")
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, cleanedPhone, agentID).Return(nil, apperrors.ErrNotFound).Once()
	contactRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Contact")).Return(nil).Once() // Contact save succeeds
	logRepo.On("FindByMessageID", mock.Anything, messageID).Return(nil, apperrors.ErrNotFound).Once()
	logRepo.On("Save", mock.Anything, mock.AnythingOfType("model.OnboardingLog")).Return(fmt.Errorf("%w: %w", apperrors.ErrDatabase, dbError)).Once()

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertExpectations(t)
	logRepo.AssertExpectations(t)
	assert.True(t, observedLogs.FilterMessage("Error saving new onboarding log").Len() == 1, "Expected error log")
}

func TestProcessOnboardingTask_SkipNotApplicable_OutFlow(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-outflow"
	messageID := "msg-outflow"
	phoneNum := "123"
	agentID := "agent-outflow"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	taskData.Message.Flow = "OUT" // Set non-IN flow
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertNotCalled(t, "FindByPhoneAndAgentID", mock.Anything, mock.Anything, mock.Anything)
	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	assert.True(t, observedLogs.FilterMessage("Skipping onboarding task: not applicable message type").Len() == 1, "Expected skip log")
}

func TestProcessOnboardingTask_SkipNotApplicable_EmptyFrom(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-emptyfrom"
	messageID := "msg-emptyfrom"
	phoneNum := "" // Empty From field
	agentID := "agent-emptyfrom"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertNotCalled(t, "FindByPhoneAndAgentID", mock.Anything, mock.Anything, mock.Anything)
	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	assert.True(t, observedLogs.FilterMessage("Skipping onboarding task: not applicable message type").Len() == 1, "Expected skip log")
}

func TestProcessOnboardingTask_SkipEmptyCleanedPhone(t *testing.T) {
	worker, contactRepo, logRepo, observedLogs := setupOnboardingWorkerTest(t)
	companyID := "tenant-emptyclean"
	messageID := "msg-emptyclean"
	phoneNum := "+()- " // Cleans to empty
	agentID := "agent-emptyclean"

	taskData := createTestOnboardingTaskData(context.Background(), companyID, messageID, phoneNum, agentID)
	ctx := tenant.WithCompanyID(taskData.Ctx, companyID)
	ctx = logger.WithLogger(ctx, worker.baseLogger)

	// Execute
	worker.processOnboardingTask(taskData)

	// Assert
	contactRepo.AssertNotCalled(t, "FindByPhoneAndAgentID", mock.Anything, mock.Anything, mock.Anything)
	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "FindByMessageID", mock.Anything, mock.Anything)
	logRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
	assert.True(t, observedLogs.FilterMessage("Skipping onboarding task: empty phone number after cleaning").Len() == 1, "Expected skip log")
}

func TestCleanPhoneNumber(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{"Simple number", "1234567890", "1234567890"},
		{"With plus", "+1234567890", "1234567890"},
		{"With hyphens", "123-456-7890", "1234567890"},
		{"With spaces", " 123 456 7890 ", "1234567890"},
		{"With parentheses", "+1 (234) 567-890", "1234567890"},
		{"Mixed symbols", "+(1) 2-3 4 ", "1234"},
		{"Empty", "", ""},
		{"Only symbols", "+()- ", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, cleanPhoneNumber(tc.input))
		})
	}
}
