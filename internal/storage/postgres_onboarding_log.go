package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
	//"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils" // Not needed unless UpdatedAt is added
)

// --- Onboarding Log Repository Methods ---

// SaveOnboardingLog creates a new onboarding log record.
// Logs are typically append-only, so this performs a direct insert.
func (r *PostgresRepo) SaveOnboardingLog(ctx context.Context, logEntry model.OnboardingLog) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	// Ensure CompanyID matches tenant context
	if companyID != logEntry.CompanyID {
		return fmt.Errorf("%w: logEntry CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, logEntry.CompanyID, companyID)
	}

	// Note: GORM automatically sets CreatedAt if `autoCreateTime` tag is present.

	operation := func() error {
		result := r.db.WithContext(ctx).Create(&logEntry)
		if result.Error != nil {
			// Use helper to check for specific constraint errors like unique violation
			return checkConstraintViolation(result.Error)
		}
		if result.RowsAffected == 0 {
			// This shouldn't happen on a successful create unless there's a hook preventing it.
			loggerCtx.Warn("SaveOnboardingLog resulted in 0 rows affected", zap.String("message_id", logEntry.MessageID))
			// Consider if this is an error state - for create, it often is.
			return fmt.Errorf("%w: create operation affected 0 rows", apperrors.ErrDatabase)
		}
		return nil
	}

	// Use standard commit policy, as it's a single insert.
	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "SaveOnboardingLog", operation)
	observer.ObserveDbOperationDuration("save", "onboarding_log", companyID, time.Since(startTime), commitErr)

	if commitErr != nil {
		loggerCtx.Error("Failed to save onboarding log after retries",
			zap.String("message_id", logEntry.MessageID),
			zap.String("company_id", companyID),
			zap.Error(commitErr))
		return commitErr // Already wrapped
	}
	return nil
}

// FindOnboardingLogByMessageID finds a specific onboarding log entry by the MessageID that triggered it.
func (r *PostgresRepo) FindOnboardingLogByMessageID(ctx context.Context, messageID string) (*model.OnboardingLog, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var logEntry model.OnboardingLog
	operation := func() error {
		result := r.db.WithContext(ctx).Where("message_id = ? AND company_id = ?", messageID, companyID).First(&logEntry)
		if result.Error != nil {
			return checkConstraintViolation(result.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindOnboardingLogByMessageID", operation)
	observer.ObserveDbOperationDuration("find_by_message_id", "onboarding_log", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		if errors.Is(findErr, apperrors.ErrNotFound) {
			return nil, apperrors.ErrNotFound
		}
		loggerCtx.Error("Failed to find onboarding log by message_id after retries",
			zap.String("message_id", messageID),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}
	return &logEntry, nil
}

// FindOnboardingLogsByPhoneNumber finds all onboarding log entries for a specific phone number.
func (r *PostgresRepo) FindOnboardingLogsByPhoneNumber(ctx context.Context, phoneNumber string) ([]model.OnboardingLog, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var logEntries []model.OnboardingLog
	operation := func() error {
		result := r.db.WithContext(ctx).Where("phone_number = ? AND company_id = ?", phoneNumber, companyID).Find(&logEntries)
		if result.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil // Success, even if no records found
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindOnboardingLogsByPhoneNumber", operation)
	observer.ObserveDbOperationDuration("find_by_phone_number", "onboarding_log", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		loggerCtx.Error("Failed to find onboarding logs by phone number after retries",
			zap.String("phone_number", phoneNumber),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}
	if logEntries == nil { // Ensure empty slice is returned, not nil
		return []model.OnboardingLog{}, nil
	}
	return logEntries, nil
}

// FindOnboardingLogsByAgentID finds all onboarding log entries associated with a specific agent ID.
func (r *PostgresRepo) FindOnboardingLogsByAgentID(ctx context.Context, agentID string) ([]model.OnboardingLog, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var logEntries []model.OnboardingLog
	operation := func() error {
		result := r.db.WithContext(ctx).Where("agent_id = ? AND company_id = ?", agentID, companyID).Find(&logEntries)
		if result.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil // Success, even if no records found
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindOnboardingLogsByAgentID", operation)
	observer.ObserveDbOperationDuration("find_by_agent_id", "onboarding_log", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		loggerCtx.Error("Failed to find onboarding logs by agent_id after retries",
			zap.String("agent_id", agentID),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}
	if logEntries == nil { // Ensure empty slice is returned, not nil
		return []model.OnboardingLog{}, nil
	}
	return logEntries, nil
}

// FindOnboardingLogsWithinTimeRange finds onboarding logs where the Timestamp falls within the specified range (inclusive).
func (r *PostgresRepo) FindOnboardingLogsWithinTimeRange(ctx context.Context, startTimeUnix, endTimeUnix int64) ([]model.OnboardingLog, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var logEntries []model.OnboardingLog
	operation := func() error {
		result := r.db.WithContext(ctx).
			Where("timestamp >= ? AND timestamp <= ? AND company_id = ?", startTimeUnix, endTimeUnix, companyID).
			Order("timestamp ASC"). // Optional: order by timestamp
			Find(&logEntries)
		if result.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil // Success, even if no records found
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindOnboardingLogsWithinTimeRange", operation)
	observer.ObserveDbOperationDuration("find_within_time_range", "onboarding_log", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		loggerCtx.Error("Failed to find onboarding logs within time range after retries",
			zap.Int64("start_time_unix", startTimeUnix),
			zap.Int64("end_time_unix", endTimeUnix),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}
	if logEntries == nil { // Ensure empty slice is returned, not nil
		return []model.OnboardingLog{}, nil
	}
	return logEntries, nil
}
