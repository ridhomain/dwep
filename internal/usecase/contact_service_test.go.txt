package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

func init() {
	// Initialize logger for tests
	logger.Log = zaptest.NewLogger(nil).Named("test")
}

// --- ProcessHistoricalContacts Tests --- //

func TestProcessHistoricalContacts_Success(t *testing.T) {
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
	companyID := "tenant-1"
	nowEpoch := time.Now().Unix()
	contactsPayload := []model.UpsertContactPayload{
		{
			PhoneNumber: "+1111111111",
			CompanyID:   companyID,
			AgentID:     "agent-A",
			PushName:    "Customer A",
		},
		{
			PhoneNumber: "+2222222222",
			CompanyID:   companyID,
			AgentID:     "agent-B",
			PushName:    "+2222222222",
			// DateOfBirth: 0, // Test with zero DOB
		},
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 10,
		StreamSequence:   20,
		CompanyID:        companyID,
	}

	// Setup expectations
	contactRepo.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Contact")).Return(nil)

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.ProcessHistoricalContacts(ctx, contactsPayload, metadata)

	// Assert
	assert.NoError(t, err)
	contactRepo.AssertCalled(t, "BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Contact"))

	// Verify the transformed data
	calls := contactRepo.Calls
	require.GreaterOrEqual(t, len(calls), 1)
	dbContacts := calls[len(calls)-1].Arguments.Get(1).([]model.Contact)
	assert.Len(t, dbContacts, 2)

	assert.Equal(t, "contact-1", dbContacts[0].ID)
	assert.Equal(t, companyID, dbContacts[0].CompanyID)
	assert.Equal(t, "Test Contact 1", dbContacts[0].CustomName)
	assert.NotNil(t, dbContacts[0].Dob)
	assert.Equal(t, nowEpoch, dbContacts[0].Dob.Unix())
	assert.NotNil(t, dbContacts[0].LastMetadata)
	assert.Equal(t, "msg-abc", dbContacts[0].FirstMessageID)

	assert.Equal(t, "contact-2", dbContacts[1].ID)
	assert.Equal(t, companyID, dbContacts[1].CompanyID)
	assert.Nil(t, dbContacts[1].Dob) // DOB should be nil if input was 0
	assert.NotNil(t, dbContacts[1].LastMetadata)
}

func TestProcessHistoricalContacts_EmptySlice(t *testing.T) {
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
	ctx = tenant.WithCompanyID(ctx, "tenant-1")

	// Call function with empty slice
	err := service.ProcessHistoricalContacts(ctx, []model.UpsertContactPayload{}, nil)

	// Assert
	assert.NoError(t, err, "Expected no error for empty slice")
	assert.Equal(t, 1, observedLogs.Len(), "Expected one log message")
	assert.Equal(t, "No contacts to process in historical contacts payload", observedLogs.All()[0].Message)

	// Verify no calls to repository methods
	contactRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
}

func TestProcessHistoricalContacts_ValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data - contact without required fields
	companyID := "tenant-1"
	contactsPayload := []model.UpsertContactPayload{
		{
			PhoneNumber: "628888888845",
			// Missing PhoneNumber, CompanyID, AgentID
		},
	}

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, companyID)

	// Call function
	err := service.ProcessHistoricalContacts(ctx, contactsPayload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "validation failed for historical contact at index 0")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "Validation failed for historical contact", observedLogs.All()[0].Message)

	contactRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
}

func TestProcessHistoricalContacts_CompanyIDValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data - CompanyID mismatch
	contextTenantID := "tenant-1"
	payloadCompanyID := "tenant-2"
	contactsPayload := []model.UpsertContactPayload{
		{
			PushName:    "cust-A",
			PhoneNumber: "+111",
			CompanyID:   payloadCompanyID, // Mismatch
			AgentID:     "agent-A",
		},
	}

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, contextTenantID)

	// Call function
	err := service.ProcessHistoricalContacts(ctx, contactsPayload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "company validation failed for historical contact at index 0")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "CompanyID validation failed for historical contact", observedLogs.All()[0].Message)
	assert.Equal(t, payloadCompanyID, observedLogs.All()[0].ContextMap()["company_id"])
	assert.Equal(t, contextTenantID, observedLogs.All()[0].ContextMap()["context_company_id"])

	contactRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
}

func TestProcessHistoricalContacts_TenantContextError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data - valid payload
	contactsPayload := []model.UpsertContactPayload{
		{
			PushName:    "cust-A",
			PhoneNumber: "+111",
			CompanyID:   "tenant-1",
			AgentID:     "agent-A",
		},
	}

	// Setup context WITHOUT tenant ID
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)

	// Call function
	err := service.ProcessHistoricalContacts(ctx, contactsPayload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "failed to get tenant ID from context")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "Failed to get tenant ID from context", observedLogs.All()[0].Message)

	contactRepo.AssertNotCalled(t, "BulkUpsert", mock.Anything, mock.Anything)
}

func TestProcessHistoricalContacts_RepoError(t *testing.T) {
	// Table-driven test for different repo errors
	testCases := []struct {
		name            string
		repoErr         error
		expectRetryable bool
		expectFatal     bool
		originalErr     error
	}{
		{
			name:            "Retryable DB Error",
			repoErr:         fmt.Errorf("bulk db fail: %w", apperrors.ErrDatabase),
			expectRetryable: true,
			expectFatal:     false,
			originalErr:     apperrors.ErrDatabase,
		},
		{
			name:            "Fatal Bad Request Error", // Example of fatal error from repo
			repoErr:         fmt.Errorf("bad data in bulk: %w", apperrors.ErrBadRequest),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrBadRequest,
		},
		{
			name:            "Fatal Other Error",
			repoErr:         errors.New("unexpected bulk failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil,
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

			companyID := "tenant-1"
			contactsPayload := []model.UpsertContactPayload{
				{PushName: "cust-A", PhoneNumber: "+111", CompanyID: companyID, AgentID: "agent-A"},
			}

			// Setup expectations - use the error from the test case
			contactRepo.On("BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Contact")).Return(tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zap.WarnLevel) // Warn or Error
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.ProcessHistoricalContacts(ctx, contactsPayload, nil)

			// Assert
			assert.Error(t, err)
			contactRepo.AssertCalled(t, "BulkUpsert", mock.Anything, mock.AnythingOfType("[]model.Contact"))

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, retryableErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
				require.GreaterOrEqual(t, observedLogs.Len(), 1)
				assert.Equal(t, zap.WarnLevel, observedLogs.All()[0].Level)
				assert.Equal(t, "Potentially retryable error during historical contact bulk upsert", observedLogs.All()[0].Message)
			} else if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, fatalErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
				require.GreaterOrEqual(t, observedLogs.Len(), 1)
				assert.Equal(t, zap.ErrorLevel, observedLogs.All()[0].Level)
				assert.Equal(t, "Fatal error during historical contact bulk upsert", observedLogs.All()[0].Message)
			}
		})
	}
}

// --- UpsertContact Tests --- //

func TestUpsertContact_Success(t *testing.T) {
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
	companyID := "tenant-1"
	nowEpoch := time.Now().Unix()
	payload := model.UpsertContactPayload{
		PushName:    "cust-C",
		PhoneNumber: "+3333333333",
		CompanyID:   companyID,
		AgentID:     "agent-C",
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 15,
		StreamSequence:   25,
		CompanyID:        companyID,
	}

	// Setup expectations
	contactRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Contact")).Return(nil)

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.UpsertContact(ctx, payload, metadata)

	// Assert
	assert.NoError(t, err)
	contactRepo.AssertCalled(t, "Save", mock.Anything, mock.AnythingOfType("model.Contact"))

	// Verify the transformed data
	calls := contactRepo.Calls
	require.GreaterOrEqual(t, len(calls), 1)
	dbContact := calls[len(calls)-1].Arguments.Get(1).(model.Contact)
	assert.Equal(t, "contact-upsert-1", dbContact.ID)
	assert.Equal(t, companyID, dbContact.CompanyID)
	assert.Equal(t, "Upsert Contact 1", dbContact.CustomName)
	assert.NotNil(t, dbContact.Dob)
	assert.Equal(t, nowEpoch, dbContact.Dob.Unix())
	assert.NotNil(t, dbContact.LastMetadata)
	assert.Equal(t, "msg-def", dbContact.FirstMessageID)
}

func TestUpsertContact_ValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup invalid test data
	payload := model.UpsertContactPayload{
		PushName: "cust-A",
		// Missing PhoneNumber, CompanyID, AgentID
	}

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "tenant-1")

	// Call function
	err := service.UpsertContact(ctx, payload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "contact validation failed")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "Contact validation failed", observedLogs.All()[0].Message)

	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

func TestUpsertContact_CompanyIDValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data - CompanyID mismatch
	contextTenantID := "tenant-1"
	payloadCompanyID := "tenant-2"
	payload := model.UpsertContactPayload{
		PushName:    "cust-D",
		PhoneNumber: "+444",
		CompanyID:   payloadCompanyID, // Mismatch
		AgentID:     "agent-D",
	}

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, contextTenantID)

	// Call function
	err := service.UpsertContact(ctx, payload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "contact CompanyID mismatch")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "CompanyID validation failed for contact", observedLogs.All()[0].Message)
	assert.Equal(t, payloadCompanyID, observedLogs.All()[0].ContextMap()["company_id"])
	assert.Equal(t, contextTenantID, observedLogs.All()[0].ContextMap()["context_company_id"])

	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

func TestUpsertContact_TenantContextError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup valid test data
	payload := model.UpsertContactPayload{
		PushName:    "cust-E",
		PhoneNumber: "+555",
		CompanyID:   "tenant-1",
		AgentID:     "agent-E",
	}

	// Setup context WITHOUT tenant ID
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)

	// Call function
	err := service.UpsertContact(ctx, payload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "failed to get tenant ID from context")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "Failed to get tenant ID from context", observedLogs.All()[0].Message)

	contactRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

func TestUpsertContact_RepoError(t *testing.T) {
	// Table-driven test for different repo errors
	testCases := []struct {
		name            string
		repoErr         error
		expectRetryable bool
		expectFatal     bool
		originalErr     error
	}{
		{
			name:            "Retryable DB Error",
			repoErr:         fmt.Errorf("save db fail: %w", apperrors.ErrDatabase),
			expectRetryable: true,
			expectFatal:     false,
			originalErr:     apperrors.ErrDatabase,
		},
		{
			name:            "Fatal Duplicate Error",
			repoErr:         fmt.Errorf("contact exists: %w", apperrors.ErrDuplicate),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrDuplicate,
		},
		{
			name:            "Fatal Bad Request Error",
			repoErr:         fmt.Errorf("bad data: %w", apperrors.ErrBadRequest),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrBadRequest,
		},
		{
			name:            "Fatal Other Error",
			repoErr:         errors.New("unexpected save failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil,
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

			companyID := "tenant-1"
			payload := model.UpsertContactPayload{
				PushName: "cust-F", PhoneNumber: "+666", CompanyID: companyID, AgentID: "agent-F",
			}

			// Setup expectations
			contactRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Contact")).Return(tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zap.WarnLevel)
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.UpsertContact(ctx, payload, nil)

			// Assert
			assert.Error(t, err)
			contactRepo.AssertCalled(t, "Save", mock.Anything, mock.AnythingOfType("model.Contact"))

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, retryableErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
				require.GreaterOrEqual(t, observedLogs.Len(), 1)
				assert.Equal(t, zap.WarnLevel, observedLogs.All()[0].Level)
				assert.Equal(t, "Potentially retryable error during contact upsert", observedLogs.All()[0].Message)
			} else if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, fatalErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
				require.GreaterOrEqual(t, observedLogs.Len(), 1)
				assert.Equal(t, zap.ErrorLevel, observedLogs.All()[0].Level)
				assert.Equal(t, "Fatal error during contact upsert", observedLogs.All()[0].Message)
			}
		})
	}
}

// --- UpdateContact Tests --- //

func TestUpdateContact_Success(t *testing.T) {
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
	companyID := "tenant-1"
	agentID := "agent-1"
	phoneNumber := "628888888845"
	initialPushName := "Initial PN"
	updatedPushName := "Updated PN"

	updatePayload := model.UpdateContactPayload{
		AgentID:     agentID,
		CompanyID:   companyID,
		PhoneNumber: phoneNumber,
		PushName:    &updatedPushName,
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 18,
		StreamSequence:   28,
		CompanyID:        companyID,
	}

	existingContact := &model.Contact{
		PhoneNumber: phoneNumber,
		CompanyID:   companyID,
		AgentID:     agentID,
		PushName:    initialPushName,
		UpdatedAt:   time.Now().Add(-1 * time.Hour),
	}

	// Setup expectations
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, phoneNumber, agentID).Return(existingContact, nil)
	contactRepo.On("Update", mock.Anything, mock.AnythingOfType("model.Contact")).Return(nil)

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.UpdateContact(ctx, updatePayload, metadata)

	// Assert
	assert.NoError(t, err)
	contactRepo.AssertCalled(t, "FindByPhoneAndAgentID", mock.Anything, phoneNumber, agentID)
	contactRepo.AssertCalled(t, "Update", mock.Anything, mock.AnythingOfType("model.Contact"))

	// Verify the updated data passed to Update
	calls := contactRepo.Calls
	updatedContactArg := model.Contact{}
	for _, call := range calls {
		if call.Method == "Update" {
			updatedContactArg = call.Arguments.Get(1).(model.Contact)
			break
		}
	}
	require.NotEmpty(t, updatedContactArg.PhoneNumber, "Updated contact argument not found in mock calls")

	assert.Equal(t, agentID, updatedContactArg.AgentID)
	assert.Equal(t, companyID, updatedContactArg.CompanyID)
	assert.Equal(t, updatedPushName, updatedContactArg.PushName)
	assert.NotEqual(t, existingContact.UpdatedAt, updatedContactArg.UpdatedAt)
	assert.NotNil(t, updatedContactArg.LastMetadata)
}

func TestUpdateContact_NoChanges(t *testing.T) {
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
	companyID := "tenant-1"
	agentID := "agent-1"
	phoneNumber := "629999"
	initialPushName := "Initial PN"

	updatePayload := model.UpdateContactPayload{
		AgentID:     agentID,
		CompanyID:   companyID,
		PhoneNumber: phoneNumber,
		PushName:    &initialPushName, // Same name as existing
		// No other fields to change
	}

	existingContact := &model.Contact{
		PhoneNumber: phoneNumber,
		CompanyID:   companyID,
		AgentID:     agentID,
		PushName:    initialPushName,
	}

	// Setup expectations
	contactRepo.On("FindByPhoneAndAgentID", mock.Anything, phoneNumber, agentID).Return(existingContact, nil)

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function (with nil metadata)
	err := service.UpdateContact(ctx, updatePayload, nil)

	// Assert
	assert.NoError(t, err)
	contactRepo.AssertCalled(t, "FindByPhoneAndAgentID", mock.Anything, phoneNumber, agentID)
	contactRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

func TestUpdateContact_ValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup invalid payload (missing PhoneNumber)
	updatePayload := model.UpdateContactPayload{
		// Missing PhoneNumber, CompanyID
	}

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "tenant-1")

	// Call function
	err := service.UpdateContact(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "contact update validation failed")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "Contact update validation failed", observedLogs.All()[0].Message)

	contactRepo.AssertNotCalled(t, "FindByPhoneAndAgentID", mock.Anything, mock.Anything, mock.Anything)
	contactRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

func TestUpdateContact_CompanyIDValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	// Setup test data - CompanyID mismatch
	contextTenantID := "tenant-1"
	payloadCompanyID := "tenant-2"
	phoneNumber := "628888"
	agentID := "agent-1"

	updatePayload := model.UpdateContactPayload{
		PhoneNumber: phoneNumber,
		AgentID:     agentID,
		CompanyID:   payloadCompanyID, // Mismatch
	}

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, contextTenantID)

	// Call function
	err := service.UpdateContact(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "contact update CompanyID mismatch")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "CompanyID validation failed for contact update", observedLogs.All()[0].Message)
	assert.Equal(t, payloadCompanyID, observedLogs.All()[0].ContextMap()["company_id"])
	assert.Equal(t, contextTenantID, observedLogs.All()[0].ContextMap()["context_company_id"])

	contactRepo.AssertNotCalled(t, "FindByPhoneAndAgentID", mock.Anything, mock.Anything, mock.Anything)
	contactRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

func TestUpdateContact_TenantContextError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedEventRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedEventRepo, onboardingWorker)

	pushName := "cust-A"
	// Setup valid payload
	updatePayload := model.UpdateContactPayload{
		PhoneNumber: "628888",
		CompanyID:   "tenant-1",
		AgentID:     "agent-1",
		PushName:    &pushName,
	}

	// Setup context WITHOUT tenant ID
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)

	// Call function
	err := service.UpdateContact(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err)
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	assert.Contains(t, fatalErr.Error(), "failed to get tenant ID from context")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "Failed to get tenant ID from context", observedLogs.All()[0].Message)

	contactRepo.AssertNotCalled(t, "FindByPhoneAndAgentID", mock.Anything, mock.Anything, mock.Anything)
	contactRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

func TestUpdateContact_FindByPhoneAndAgentIDError(t *testing.T) {
	// Table-driven test for different FindByPhoneAndAgentID errors
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
			logMessage:      "Potentially retryable error fetching contact for update",
		},
		{
			name:            "Fatal Not Found Error on Find",
			repoErr:         apperrors.ErrNotFound, // Repo returns this directly now
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrNotFound,
			logLevel:        zapcore.WarnLevel,
			logMessage:      "Contact not found for update",
		},
		{
			name:            "Fatal Other Error on Find",
			repoErr:         errors.New("unexpected find failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil,
			logLevel:        zapcore.ErrorLevel,
			logMessage:      "Fatal error fetching contact for update",
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

			companyID := "tenant-1"
			agentID := "contact-update-find-err"
			phoneNumber := "628888"
			pushName := "cust-1"

			updatePayload := model.UpdateContactPayload{PhoneNumber: phoneNumber, CompanyID: companyID, AgentID: agentID, PushName: &pushName}

			// Setup expectations
			contactRepo.On("FindByPhoneAndAgentID", mock.Anything, phoneNumber, agentID).Return(nil, tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zap.WarnLevel)
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.UpdateContact(ctx, updatePayload, nil)

			// Assert
			assert.Error(t, err)
			contactRepo.AssertCalled(t, "FindByPhoneAndAgentID", mock.Anything, phoneNumber, agentID)
			contactRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)

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

// TestUpdateContact_ContactNotFound is now covered by TestUpdateContact_FindByPhoneAndAgentID Error

func TestUpdateContact_CompanyIDMismatch(t *testing.T) {
	// Restore the original structure of this test, focusing on the pre-FindByPhoneAndAgentID validation
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
	phoneNumber := "628888"
	agentID := "contact-update-cid-mismatch"
	pushName := "cust-A"
	contextTenantID := "tenant-1"  // Context tenant
	payloadCompanyID := "tenant-2" // Payload company ID - Mismatch

	updatePayload := model.UpdateContactPayload{
		PhoneNumber: phoneNumber,
		AgentID:     agentID,
		CompanyID:   payloadCompanyID,
		PushName:    &pushName,
	}

	// Setup expectations: FindByID should NOT be called due to early validation failure
	// contactRepo.On("FindByID", mock.Anything, contactID).Return(existingContact, nil) // Remove this expectation

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, contextTenantID) // Set context tenant

	// Call function
	err := service.UpdateContact(ctx, updatePayload, nil)

	// Assert
	assert.Error(t, err)
	// Check it's a fatal error wrapper
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	// Check the wrapped message
	assert.Contains(t, fatalErr.Error(), "contact update CompanyID mismatch")

	// Check logs
	require.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, zapcore.ErrorLevel, observedLogs.All()[0].Level)
	assert.Equal(t, "CompanyID validation failed for contact update", observedLogs.All()[0].Message)
	// existingCompanyID is not relevant here as FindByPhoneAndAgentID wasn't called
	// assert.Equal(t, existingCompanyID, observedLogs.All()[0].ContextMap()["existing_company_id"])
	assert.Equal(t, payloadCompanyID, observedLogs.All()[0].ContextMap()["company_id"])
	assert.Equal(t, contextTenantID, observedLogs.All()[0].ContextMap()["context_company_id"])

	// Verify FindByPhoneAndAgentID and Update were NOT called
	contactRepo.AssertNotCalled(t, "FindByPhoneAndAgentID", mock.Anything, mock.Anything, mock.Anything)
	contactRepo.AssertNotCalled(t, "Update", mock.Anything, mock.Anything)
}

func TestUpdateContact_UpdateRepoError(t *testing.T) {
	// Table-driven test for different Update errors
	testCases := []struct {
		name            string
		repoErr         error
		expectRetryable bool
		expectFatal     bool
		originalErr     error
	}{
		{
			name:            "Retryable DB Error on Update",
			repoErr:         fmt.Errorf("update db fail: %w", apperrors.ErrDatabase),
			expectRetryable: true,
			expectFatal:     false,
			originalErr:     apperrors.ErrDatabase,
		},
		{
			name:            "Fatal Bad Request on Update",
			repoErr:         fmt.Errorf("invalid update data: %w", apperrors.ErrBadRequest),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrBadRequest,
		},
		{
			name:            "Fatal Other Error on Update",
			repoErr:         errors.New("unexpected update failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil,
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

			companyID := "tenant-1"
			agentID := "contact-update-repo-err"
			phoneNumber := "627777"
			updatedPushName := "Update Repo Error Name"
			updatePayload := model.UpdateContactPayload{PhoneNumber: phoneNumber, CompanyID: companyID, AgentID: agentID, PushName: &updatedPushName}
			existingContact := &model.Contact{PhoneNumber: phoneNumber, CompanyID: companyID, PushName: "Original PN"}

			// Setup expectations
			contactRepo.On("FindByPhoneAndAgentID", mock.Anything, phoneNumber, agentID).Return(existingContact, nil)
			contactRepo.On("Update", mock.Anything, mock.AnythingOfType("model.Contact")).Return(tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zap.WarnLevel)
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.UpdateContact(ctx, updatePayload, nil)

			// Assert
			assert.Error(t, err)
			contactRepo.AssertCalled(t, "FindByPhoneAndAgentID", mock.Anything, phoneNumber, agentID)
			contactRepo.AssertCalled(t, "Update", mock.Anything, mock.AnythingOfType("model.Contact"))

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, retryableErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
				require.GreaterOrEqual(t, observedLogs.Len(), 1)
				assert.Equal(t, zap.WarnLevel, observedLogs.All()[0].Level)
				assert.Equal(t, "Potentially retryable error during contact update", observedLogs.All()[0].Message)
			} else if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, fatalErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
				require.GreaterOrEqual(t, observedLogs.Len(), 1)
				assert.Equal(t, zap.ErrorLevel, observedLogs.All()[0].Level)
				assert.Equal(t, "Fatal error during contact update", observedLogs.All()[0].Message)
			}
		})
	}
}
