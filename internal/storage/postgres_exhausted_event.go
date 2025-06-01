package storage

import (
	"context"
	"time"

	"go.uber.org/zap"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// SaveExhaustedEvent saves an exhausted DLQ event to the database.
// This method is part of the PostgresRepo struct defined in postgres.go
func (r *PostgresRepo) SaveExhaustedEvent(ctx context.Context, event model.ExhaustedEvent) error {
	companyID, err := tenant.FromContext(ctx) // Assuming tenant context is available here
	if err != nil {
		// Log or handle error if companyID is crucial and missing
		logger.FromContext(ctx).Warn("Failed to get tenant ID for exhausted event metric", zap.Error(err))
		companyID = "unknown" // Use a placeholder if tenant is not strictly required for this operation's metric
	}

	operation := func() error {
		// Simple create, no upsert needed for exhausted events (assuming they are unique by nature)
		result := r.db.WithContext(ctx).Create(&event)
		if result.Error != nil {
			// Check for specific DB errors if needed, otherwise wrap generically
			return checkConstraintViolation(result.Error) // Use the common error checker
		}
		return nil
	}

	// Use a standard commit retry policy
	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime) // Defined in postgres.go
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "SaveExhaustedEvent Commit", operation) // Defined in postgres.go
	observer.ObserveDbOperationDuration("save", "exhausted_event", companyID, time.Since(startTime), commitErr)

	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to save exhausted event after retries",
			zap.String("source_subject", event.SourceSubject),
			zap.String("company_id", event.CompanyID),
			zap.Error(commitErr))
		return commitErr // Already wrapped
	}

	logger.FromContext(ctx).Info("Successfully saved exhausted event", zap.Uint("event_id", event.ID), zap.String("source_subject", event.SourceSubject))
	return nil
}
