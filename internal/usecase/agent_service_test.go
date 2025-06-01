package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	storagemock "gitlab.com/timkado/api/daisi-wa-events-processor/internal/storage/mock"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
)

func init() {
	// Initialize logger for tests
	logger.Log = zaptest.NewLogger(nil).Named("test")
}

// --- UpsertAgent Tests --- //

func TestUpsertAgent_Success(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)       // Needed for NewEventService, but not used directly
	messageRepo := new(storagemock.MessageRepoMock) // Needed for NewEventService, but not used directly
	contactRepo := new(storagemock.ContactRepoMock) // Needed for NewEventService, but not used directly
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedRepo, onboardingWorker)

	// Setup test data
	companyID := "company-123"
	agentID := "agent-xyz"
	payload := model.UpsertAgentPayload{
		AgentID:   agentID,
		CompanyID: companyID,
		QRCode:    "some-qr-code",
		Status:    "connected",
		AgentName: "Test Agent",
		HostName:  "agent-host",
		Version:   "1.0.0",
	}

	metadata := &model.LastMetadata{
		ConsumerSequence: 5,
		StreamSequence:   15,
		CompanyID:        companyID,
	}

	// Setup expectations
	agentRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Agent")).Return(nil)

	// Setup context
	ctx := tenant.WithCompanyID(context.Background(), companyID)
	ctx = logger.WithLogger(ctx, zaptest.NewLogger(t))

	// Call function
	err := service.UpsertAgent(ctx, payload, metadata)

	// Assert
	assert.NoError(t, err)
	agentRepo.AssertCalled(t, "Save", mock.Anything, mock.AnythingOfType("model.Agent"))

	// Verify the transformed data
	calls := agentRepo.Calls
	require.GreaterOrEqual(t, len(calls), 1)
	dbAgent := calls[len(calls)-1].Arguments.Get(1).(model.Agent)
	assert.Equal(t, agentID, dbAgent.AgentID)
	assert.Equal(t, companyID, dbAgent.CompanyID)
	assert.Equal(t, "Test Agent", dbAgent.AgentName)
	assert.Equal(t, "connected", dbAgent.Status)
	assert.NotNil(t, dbAgent.LastMetadata)
}

func TestUpsertAgent_ValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedRepo, onboardingWorker)

	// Setup invalid test data (missing required fields)
	payload := model.UpsertAgentPayload{
		// Missing AgentID, CompanyID
		AgentName: "Invalid Agent",
	}

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, "some-tenant") // Need tenant in context for other checks

	// Call function
	err := service.UpsertAgent(ctx, payload, nil)

	// Assert
	assert.Error(t, err)
	// Check if it's a FatalError wrapping the original validation error
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	// You could further assert the underlying error if validator returned specific types
	// assert.ErrorIs(t, fatalErr.Unwrap(), specificValidationError)

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "Agent payload validation failed", observedLogs.All()[0].Message)

	agentRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

func TestUpsertAgent_CompanyIDValidationError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedRepo, onboardingWorker)

	// Setup test data - CompanyID mismatch
	contextTenantID := "company-ctx"
	payloadCompanyID := "company-payload"
	payload := model.UpsertAgentPayload{
		AgentID:   "agent-mismatch",
		CompanyID: payloadCompanyID, // Mismatch
		AgentName: "Mismatch Agent",
	}

	// Create observable logger
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)
	ctx = tenant.WithCompanyID(ctx, contextTenantID)

	// Call function
	err := service.UpsertAgent(ctx, payload, nil)

	// Assert
	assert.Error(t, err)
	// Check if it's a FatalError wrapping the original company validation error
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
	// Could assert underlying error if validateClusterTenant returned specific types

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "CompanyID validation failed for agent", observedLogs.All()[0].Message)
	assert.Equal(t, payloadCompanyID, observedLogs.All()[0].ContextMap()["payload_company_id"])
	assert.Equal(t, contextTenantID, observedLogs.All()[0].ContextMap()["context_company_id"])

	agentRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

func TestUpsertAgent_TenantContextError(t *testing.T) {
	// Setup
	chatRepo := new(storagemock.ChatRepoMock)
	messageRepo := new(storagemock.MessageRepoMock)
	contactRepo := new(storagemock.ContactRepoMock)
	agentRepo := new(storagemock.AgentRepoMock)
	onboardingRepo := new(storagemock.OnboardingLogRepoMock)
	exhaustedRepo := new(storagemock.ExhaustedEventRepoMock)
	onboardingWorker := new(MockOnboardingWorker)
	service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedRepo, onboardingWorker)

	// Setup valid test data
	payload := model.UpsertAgentPayload{
		AgentID:   "agent-no-ctx",
		CompanyID: "company-no-ctx",
		AgentName: "No Context Agent",
	}

	// Setup context WITHOUT tenant ID
	observedZapCore, observedLogs := observer.New(zap.ErrorLevel)
	observedLogger := zap.New(observedZapCore)
	ctx := logger.WithLogger(context.Background(), observedLogger)

	// Call function
	err := service.UpsertAgent(ctx, payload, nil)

	// Assert
	assert.Error(t, err)
	// Check if it's a FatalError wrapping the original tenant error
	var fatalErr *apperrors.FatalError
	assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")

	assert.GreaterOrEqual(t, observedLogs.Len(), 1)
	assert.Equal(t, "Failed to get tenant ID from context for agent upsert", observedLogs.All()[0].Message)

	agentRepo.AssertNotCalled(t, "Save", mock.Anything, mock.Anything)
}

func TestUpsertAgent_RepoError(t *testing.T) {
	// Test cases for different repository errors (Retryable vs Fatal)
	testCases := []struct {
		name            string
		repoErr         error // Error returned by mock repo
		expectRetryable bool
		expectFatal     bool
		originalErr     error // The underlying standard error expected
	}{
		{
			name:            "Retryable Database Error",
			repoErr:         fmt.Errorf("some temporary issue: %w", apperrors.ErrDatabase),
			expectRetryable: true,
			expectFatal:     false,
			originalErr:     apperrors.ErrDatabase,
		},
		{
			name:            "Fatal Duplicate Error",
			repoErr:         fmt.Errorf("agent already exists: %w", apperrors.ErrDuplicate),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrDuplicate,
		},
		{
			name:            "Fatal Bad Request Error",
			repoErr:         fmt.Errorf("tenant mismatch from repo: %w", apperrors.ErrBadRequest),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrBadRequest,
		},
		{
			name:            "Fatal Unauthorized Error",
			repoErr:         fmt.Errorf("missing tenant in repo layer: %w", apperrors.ErrUnauthorized),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     apperrors.ErrUnauthorized,
		},
		{
			name:            "Fatal Other Error",
			repoErr:         errors.New("some unexpected repo failure"),
			expectRetryable: false,
			expectFatal:     true,
			originalErr:     nil, // originalErr is not a standard one we know
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
			exhaustedRepo := new(storagemock.ExhaustedEventRepoMock)
			onboardingWorker := new(MockOnboardingWorker)
			service := NewEventService(chatRepo, messageRepo, contactRepo, agentRepo, onboardingRepo, exhaustedRepo, onboardingWorker)

			companyID := "company-repo-err"
			agentID := "agent-repo-err"
			payload := model.UpsertAgentPayload{
				AgentID:   agentID,
				CompanyID: companyID,
				AgentName: "Repo Error Agent",
			}

			// Setup expectations - use the error from the test case
			agentRepo.On("Save", mock.Anything, mock.AnythingOfType("model.Agent")).Return(tc.repoErr)

			// Setup context
			observedZapCore, observedLogs := observer.New(zap.WarnLevel) // Capture Warn and Error
			observedLogger := zap.New(observedZapCore)
			ctx := logger.WithLogger(context.Background(), observedLogger)
			ctx = tenant.WithCompanyID(ctx, companyID)

			// Call function
			err := service.UpsertAgent(ctx, payload, nil)

			// Assert
			assert.Error(t, err)
			agentRepo.AssertCalled(t, "Save", mock.Anything, mock.AnythingOfType("model.Agent"))

			if tc.expectRetryable {
				var retryableErr *apperrors.RetryableError
				assert.True(t, errors.As(err, &retryableErr), "Expected RetryableError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, retryableErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
				// Check logs for Warn level
				require.GreaterOrEqual(t, observedLogs.Len(), 1)
				assert.Equal(t, zap.WarnLevel, observedLogs.All()[0].Level)
				assert.Equal(t, "Potentially retryable error during agent upsert", observedLogs.All()[0].Message)
			} else if tc.expectFatal {
				var fatalErr *apperrors.FatalError
				assert.True(t, errors.As(err, &fatalErr), "Expected FatalError")
				if tc.originalErr != nil {
					assert.ErrorIs(t, fatalErr.Unwrap(), tc.originalErr, "Underlying error mismatch")
				}
				// Check logs for Error level
				require.GreaterOrEqual(t, observedLogs.Len(), 1)
				assert.Equal(t, zap.ErrorLevel, observedLogs.All()[0].Level)
				// Message could be 'Fatal repository error...' or 'Unexpected repository error...'
				assert.Equal(t, "Fatal error during agent upsert", observedLogs.All()[0].Message)
			}
		})
	}
}
