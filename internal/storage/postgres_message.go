package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/observer"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// SaveMessage stores a message in the database
func (r *PostgresRepo) SaveMessage(ctx context.Context, message model.Message) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	if companyID != message.CompanyID {
		return fmt.Errorf("%w: message CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, message.CompanyID, companyID)
	}

	message.MessageDate = model.CreateTimeFromTimestamp(message.MessageTimestamp) // Populate message_date
	message.UpdatedAt = utils.Now()                                               // Ensure UpdatedAt is set for potential update

	operation := func() error {
		// Use Clauses for ON CONFLICT behavior (UPSERT)
		// Assumes a unique constraint on (message_id, company_id)
		result := r.db.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "message_id"}, {Name: "message_date"}}, // Conflict target
			// Update all columns except primary key and created_at on conflict
			DoUpdates: clause.AssignmentColumns(message.GetUpdatableFields()),
		}).Create(&message)

		if result.Error != nil {
			return checkConstraintViolation(result.Error) // Wrap potential errors
		}
		// Optional: Check RowsAffected if needed, though UPSERT might return 0 or 1
		// logger.FromContext(ctx).Debug("SaveMessage UPSERT rows affected", zap.Int64("rows_affected", result.RowsAffected))
		return nil // Success
	}

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "SaveMessage Commit", operation)
	observer.ObserveDbOperationDuration("upsert", "message", companyID, time.Since(startTime), commitErr)

	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to save message after retries", zap.String("message_id", message.MessageID), zap.Error(commitErr))
		return commitErr // Already wrapped
	}

	return nil
}

// UpdateMessage updates a message in the database
func (r *PostgresRepo) UpdateMessage(ctx context.Context, message model.Message) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	if companyID != message.CompanyID {
		return fmt.Errorf("%w: message CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, message.CompanyID, companyID)
	}

	message.UpdatedAt = utils.Now() // Ensure UpdatedAt is set
	// Ensure MessageDate is set, especially if it might change (though unlikely for update)
	message.MessageDate = model.CreateTimeFromTimestamp(message.MessageTimestamp)

	operation := func() error {
		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
				panic(r)
			} else if txErr != nil {
				if rbErr := tx.Rollback().Error; rbErr != nil {
					logger.FromContext(ctx).Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}()

		var existingMessage model.Message
		result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("message_id = ? AND company_id = ?", message.MessageID, message.CompanyID).
			First(&existingMessage)
		findErr := result.Error

		if findErr != nil {
			if errors.Is(findErr, gorm.ErrRecordNotFound) {
				txErr = fmt.Errorf("%w: message not found for update (MessageID: %s, CompanyID: %s): %w", apperrors.ErrNotFound, message.MessageID, message.CompanyID, findErr)
				return backoff.Permanent(txErr)
			}
			txErr = fmt.Errorf("%w: failed to lock message row for update: %w", apperrors.ErrDatabase, findErr)
			return txErr
		}

		message.ID = existingMessage.ID               // Ensure PK is set
		message.CreatedAt = existingMessage.CreatedAt // Preserve created_at
		updateResult := tx.Model(&existingMessage).Updates(message)
		if updateResult.Error != nil {
			txErr = checkConstraintViolation(updateResult.Error)
			return txErr
		}

		if updateResult.RowsAffected == 0 {
			logger.FromContext(ctx).Warn("UpdateMessage resulted in 0 rows affected", zap.String("message_id", message.MessageID))
		}

		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = checkConstraintViolation(commitErr)
			return txErr
		}
		return nil // Success
	}

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "UpdateMessage Commit", operation)
	observer.ObserveDbOperationDuration("update", "message", companyID, time.Since(startTime), commitErr)

	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to update message after retries", zap.String("message_id", message.MessageID), zap.Error(commitErr))
		return commitErr // Already wrapped
	}

	return nil
}

// FindMessageByMessageID finds a message by ID
func (r *PostgresRepo) FindMessageByMessageID(ctx context.Context, messageID string) (*model.Message, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var message model.Message
	operation := func() error {
		result := r.db.WithContext(ctx).Where("message_id = ? AND company_id = ?", messageID, companyID).First(&message)
		if result.Error != nil {
			return checkConstraintViolation(result.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindMessageByMessageID", operation)
	observer.ObserveDbOperationDuration("find", "message", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		if errors.Is(findErr, apperrors.ErrNotFound) {
			return nil, apperrors.ErrNotFound
		}
		loggerCtx.Error("Failed to find message by message_id after retries",
			zap.String("message_id", messageID),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}

	return &message, nil
}

// BulkUpsertMessages performs a bulk upsert of messages
func (r *PostgresRepo) BulkUpsertMessages(ctx context.Context, messages []model.Message) error {
	if len(messages) == 0 {
		return nil
	}

	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	validMessages := make([]model.Message, 0, len(messages))
	for i := range messages {
		if companyID != messages[i].CompanyID {
			loggerCtx.Warn("Company ID mismatch for message, skipping",
				zap.String("message_id", messages[i].MessageID),
				zap.String("context_company_id", companyID),
				zap.String("message_company_id", messages[i].CompanyID))
			continue
		}
		messages[i].MessageDate = model.CreateTimeFromTimestamp(messages[i].MessageTimestamp) // Populate message_date
		messages[i].UpdatedAt = utils.Now()
		validMessages = append(validMessages, messages[i])
	}

	if len(validMessages) == 0 {
		loggerCtx.Info("No valid messages found for bulk upsert after tenant ID filtering")
		return nil
	}

	// Bulk upsert using ON CONFLICT
	operation := func() error {
		tx := r.db.WithContext(ctx).Begin()
		if tx.Error != nil {
			return fmt.Errorf("%w: failed to begin transaction: %w", apperrors.ErrDatabase, tx.Error)
		}
		var txErr error
		defer func() {
			if r := recover(); r != nil {
				tx.Rollback()
				panic(r)
			} else if txErr != nil {
				if rbErr := tx.Rollback().Error; rbErr != nil {
					loggerCtx.Error("Failed to rollback transaction after error", zap.Error(rbErr), zap.NamedError("originalTxError", txErr))
				}
			}
		}()

		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "message_id"}, {Name: "message_date"}}, // Conflict target
			DoUpdates: clause.AssignmentColumns(model.MessageUpdatableFields()),      // Update all columns except primary key and created_at
		}).Create(&validMessages)

		if result.Error != nil {
			// Wrap potentially masked errors from OnConflict
			txErr = fmt.Errorf("%w: bulk upsert messages failed: %w", apperrors.ErrDatabase, result.Error)
			return txErr
		}

		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = checkConstraintViolation(commitErr)
			return txErr
		}
		loggerCtx.Info("Bulk upsert messages successful", zap.Int("messages_processed", len(validMessages)), zap.Int64("rows_affected", result.RowsAffected))
		return nil // Success
	}

	commitPolicy := newRetryPolicy(ctx, bulkCommitRetryMaxElapsedTime) // Use potentially longer timeout for bulk
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "BulkUpsertMessages Commit", operation)
	observer.ObserveDbOperationDuration("bulk_upsert", "message", companyID, time.Since(startTime), commitErr)

	if commitErr != nil {
		loggerCtx.Error("Failed to bulk upsert messages after retries", zap.Error(commitErr))
		return commitErr // Already wrapped
	}

	return nil
}

// FindMessagesByJID finds messages by JID within a date range
func (r *PostgresRepo) FindMessagesByJID(ctx context.Context, jid string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var messages []model.Message
	operation := func() error {
		query := r.db.WithContext(ctx).
			Where("jid = ? AND company_id = ? AND message_date >= ? AND message_date <= ?", jid, companyID, startDate.Format("2006-01-02"), endDate.Format("2006-01-02")).
			Order("message_timestamp DESC").
			Limit(limit).
			Offset(offset).
			Find(&messages)
		if query.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, query.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	err = retryableOperation(ctx, readPolicy, "FindMessagesByJID", operation)
	observer.ObserveDbOperationDuration("find_by_jid", "message", companyID, time.Since(startTime), err)

	if err != nil {
		loggerCtx.Error("Failed to find messages by JID after retries",
			zap.String("jid", jid),
			zap.Time("startDate", startDate),
			zap.Time("endDate", endDate),
			zap.String("company_id", companyID),
			zap.Error(err))
		return nil, err // Already wrapped
	}
	return messages, nil
}

// FindMessagesBySender finds messages by sender address within a date range
func (r *PostgresRepo) FindMessagesBySender(ctx context.Context, sender string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var messages []model.Message
	operation := func() error {
		query := r.db.WithContext(ctx).
			Where(`"from" = ? AND company_id = ? AND message_date >= ? AND message_date <= ?`, sender, companyID, startDate.Format("2006-01-02"), endDate.Format("2006-01-02")).
			Order("message_timestamp DESC").
			Limit(limit).
			Offset(offset).
			Find(&messages)
		if query.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, query.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	err = retryableOperation(ctx, readPolicy, "FindMessagesBySender", operation)
	observer.ObserveDbOperationDuration("find_by_sender", "message", companyID, time.Since(startTime), err)

	if err != nil {
		loggerCtx.Error("Failed to find messages by sender after retries",
			zap.String("sender", sender),
			zap.Time("startDate", startDate),
			zap.Time("endDate", endDate),
			zap.String("company_id", companyID),
			zap.Error(err))
		return nil, err // Already wrapped
	}
	return messages, nil
}

// FindMessagesByFromPhone finds messages by the 'from' field (user identifier) within a date range
func (r *PostgresRepo) FindMessagesByFromPhone(ctx context.Context, FromPhone string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var messages []model.Message
	operation := func() error {
		query := r.db.WithContext(ctx).
			Where(`"from" = ? AND company_id = ? AND message_date >= ? AND message_date <= ?`, FromPhone, companyID, startDate.Format("2006-01-02"), endDate.Format("2006-01-02")).
			Order("message_timestamp DESC").
			Limit(limit).
			Offset(offset).
			Find(&messages)
		if query.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, query.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	err = retryableOperation(ctx, readPolicy, "FindMessagesByFromPhone", operation)
	observer.ObserveDbOperationDuration("find_by_from_phone", "message", companyID, time.Since(startTime), err)

	if err != nil {
		loggerCtx.Error("Failed to find messages by from user after retries",
			zap.String("from_phone", FromPhone),
			zap.Time("startDate", startDate),
			zap.Time("endDate", endDate),
			zap.String("company_id", companyID),
			zap.Error(err))
		return nil, err // Already wrapped
	}
	return messages, nil
}

// FindMessagesByToPhone finds messages by the 'to' field (recipient identifier) within a date range
func (r *PostgresRepo) FindMessagesByToPhone(ctx context.Context, ToPhone string, startDate, endDate time.Time, limit int, offset int) ([]model.Message, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var messages []model.Message
	operation := func() error {
		query := r.db.WithContext(ctx).
			Where(`"to_phone" = ? AND company_id = ? AND message_date >= ? AND message_date <= ?`, ToPhone, companyID, startDate.Format("2006-01-02"), endDate.Format("2006-01-02")).
			Order("message_timestamp DESC").
			Limit(limit).
			Offset(offset).
			Find(&messages)
		if query.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, query.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	err = retryableOperation(ctx, readPolicy, "FindMessagesByToPhone", operation)
	observer.ObserveDbOperationDuration("find_by_to_phone", "message", companyID, time.Since(startTime), err)

	if err != nil {
		loggerCtx.Error("Failed to find messages by to user after retries",
			zap.String("to_phone", ToPhone),
			zap.Time("startDate", startDate),
			zap.Time("endDate", endDate),
			zap.String("company_id", companyID),
			zap.Error(err))
		return nil, err // Already wrapped
	}
	return messages, nil
}
