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

// SaveChat stores a chat in the database
func (r *PostgresRepo) SaveChat(ctx context.Context, chat model.Chat) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}

	if companyID != chat.CompanyID {
		return fmt.Errorf("%w: chat CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, chat.CompanyID, companyID)
	}

	chat.UpdatedAt = utils.Now() // Ensure UpdatedAt is set for potential update

	operation := func() error {
		// Use Clauses for ON CONFLICT behavior (UPSERT)
		// Assumes a unique constraint on (chat_id, company_id)
		result := r.db.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "chat_id"}},
			// Update specific columns on conflict
			DoUpdates: clause.AssignmentColumns(chat.GetUpdatableFields()),
		}).Create(&chat)

		if result.Error != nil {
			return checkConstraintViolation(result.Error) // Wrap potential errors
		}
		return nil // Success
	}

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "SaveChat Commit", operation)
	observer.ObserveDbOperationDuration("upsert", "chat", companyID, time.Since(startTime), commitErr)

	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to save chat after retries", zap.String("chat_id", chat.ChatID), zap.Error(commitErr))
		return commitErr // Already wrapped
	}

	return nil
}

// UpdateChat updates a chat in the database
func (r *PostgresRepo) UpdateChat(ctx context.Context, chat model.Chat) error {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}

	if companyID != chat.CompanyID {
		return fmt.Errorf("%w: chat CompanyID %s does not match tenant ID %s", apperrors.ErrBadRequest, chat.CompanyID, companyID)
	}

	chat.UpdatedAt = utils.Now() // Ensure UpdatedAt is set

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

		var existingChat model.Chat
		result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("chat_id = ? AND company_id = ?", chat.ChatID, companyID).
			First(&existingChat)
		findErr := result.Error

		if findErr != nil {
			if errors.Is(findErr, gorm.ErrRecordNotFound) {
				// Record not found is a permanent error for Update
				txErr = fmt.Errorf("%w: chat not found for update (ChatID: %s, CompanyID: %s): %w", apperrors.ErrNotFound, chat.ChatID, companyID, findErr)
				return backoff.Permanent(txErr) // Make NotFound permanent for retry policy
			}
			// Other errors during lock acquisition
			txErr = fmt.Errorf("%w: failed to lock chat row for update: %w", apperrors.ErrDatabase, findErr)
			return txErr
		}

		// Create updatefields map + real update checker
		updateFields := map[string]interface{}{}
		now := utils.Now()
		hasRealUpdate := false

		// Compare and add only changed fields to the update map
		// if chat.PushName != existingChat.PushName {
		// 	updateFields["push_name"] = chat.PushName
		// }

		// CRITICAL: Always update UnreadCount if it's different, even if it's 0
		if chat.UnreadCount != existingChat.UnreadCount {
			updateFields["unread_count"] = chat.UnreadCount
			hasRealUpdate = true
			logger.FromContext(ctx).Debug("Updating unread count",
				zap.String("chat_id", chat.ChatID),
				zap.Int32("old_count", existingChat.UnreadCount),
				zap.Int32("new_count", chat.UnreadCount))
		}
		if chat.ConversationTimestamp != 0 && chat.ConversationTimestamp != existingChat.ConversationTimestamp {
			updateFields["conversation_timestamp"] = chat.ConversationTimestamp
			hasRealUpdate = true
			logger.FromContext(ctx).Debug("Updating conversation timestamp",
				zap.String("chat_id", chat.ChatID),
				zap.Int64("old_ts", existingChat.ConversationTimestamp),
				zap.Int64("new_ts", chat.ConversationTimestamp))
		}
		if chat.LastMessageObj != nil {
			hasRealUpdate = true
			updateFields["last_message"] = chat.LastMessageObj
		}
		if hasRealUpdate && chat.LastMetadata != nil {
			updateFields["last_metadata"] = chat.LastMetadata
			logger.FromContext(ctx).Debug("Including metadata with update",
				zap.String("chat_id", chat.ChatID))
		}

		// Only proceed with update if there are fields to update
		if !hasRealUpdate {
			logger.FromContext(ctx).Debug("No meaningful fields to update for chat", zap.String("chat_id", chat.ChatID))
			// Still update updated_at
			existingChat.UpdatedAt = now
			if commitErr := tx.Save(&existingChat).Error; commitErr != nil {
				txErr = checkConstraintViolation(commitErr)
				return txErr
			}
			return nil
		}

		// Perform the update using map instead of struct
		updateFields["updated_at"] = now
		updateResult := tx.Model(&existingChat).Updates(updateFields)
		if updateResult.Error != nil {
			txErr = checkConstraintViolation(updateResult.Error)
			return txErr
		}

		if updateResult.RowsAffected == 0 {
			logger.FromContext(ctx).Warn("UpdateChat resulted in 0 rows affected",
				zap.String("chat_id", chat.ChatID),
				zap.Any("update_fields", updateFields))
		} else {
			logger.FromContext(ctx).Debug("Chat updated successfully",
				zap.String("chat_id", chat.ChatID),
				zap.Int64("rows_affected", updateResult.RowsAffected),
				zap.Int("fields_updated", len(updateFields)))
		}

		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = checkConstraintViolation(commitErr)
			return txErr
		}
		return nil
	}

	commitPolicy := newRetryPolicy(ctx, commitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "UpdateChat Commit", operation)
	observer.ObserveDbOperationDuration("update", "chat", companyID, time.Since(startTime), commitErr)

	if commitErr != nil {
		logger.FromContext(ctx).Error("Failed to update chat after retries", zap.String("chat_id", chat.ChatID), zap.Error(commitErr))
		return commitErr // Already wrapped
	}

	return nil
}

// FindChatByChatID finds a chat by ID
func (r *PostgresRepo) FindChatByChatID(ctx context.Context, chatID string) (*model.Chat, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var chat model.Chat
	operation := func() error {
		result := r.db.WithContext(ctx).Where("chat_id = ? AND company_id = ?", chatID, companyID).First(&chat)
		if result.Error != nil {
			return checkConstraintViolation(result.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindChatByChatID", operation)
	observer.ObserveDbOperationDuration("find", "chat", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		if errors.Is(findErr, apperrors.ErrNotFound) {
			return nil, apperrors.ErrNotFound
		}
		loggerCtx.Error("Failed to find chat by chat_id after retries",
			zap.String("chat_id", chatID),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}

	return &chat, nil
}

// FindChatsByJID finds chats by JID within the tenant context
func (r *PostgresRepo) FindChatsByJID(ctx context.Context, jid string) ([]model.Chat, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var chats []model.Chat
	operation := func() error {
		result := r.db.WithContext(ctx).Where("jid = ? AND company_id = ?", jid, companyID).Find(&chats)
		if result.Error != nil {
			// Find multiple doesn't return ErrRecordNotFound
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil // Success, even if no records found
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindChatsByJID", operation)
	observer.ObserveDbOperationDuration("find_by_jid", "chat", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		loggerCtx.Error("Failed to find chats by jid after retries",
			zap.String("jid", jid),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}

	return chats, nil // Return the potentially empty slice
}

// FindChatsByAgentID finds chats by Agent ID within the tenant context
func (r *PostgresRepo) FindChatsByAgentID(ctx context.Context, agentID string) ([]model.Chat, error) {
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to get tenant ID: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	var chats []model.Chat
	operation := func() error {
		result := r.db.WithContext(ctx).Where("agent_id = ? AND company_id = ?", agentID, companyID).Find(&chats)
		if result.Error != nil {
			return fmt.Errorf("%w: query failed: %w", apperrors.ErrDatabase, result.Error)
		}
		return nil
	}

	readPolicy := newRetryPolicy(ctx, readRetryMaxElapsedTime)
	startTime := utils.Now()
	findErr := retryableOperation(ctx, readPolicy, "FindChatsByAgentID", operation)
	observer.ObserveDbOperationDuration("find_by_agent_id", "chat", companyID, time.Since(startTime), findErr)

	if findErr != nil {
		loggerCtx.Error("Failed to find chats by agent_id after retries",
			zap.String("agent_id", agentID),
			zap.String("company_id", companyID),
			zap.Error(findErr))
		return nil, findErr // Already wrapped
	}

	return chats, nil // Return the potentially empty slice
}

// BulkUpsertChats performs a bulk upsert operation for chat records.
func (r *PostgresRepo) BulkUpsertChats(ctx context.Context, chats []model.Chat) error {
	if len(chats) == 0 {
		return nil
	}
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: failed to get tenant ID from context: %w", apperrors.ErrUnauthorized, err)
	}
	loggerCtx := logger.FromContext(ctx)

	validChats := make([]model.Chat, 0, len(chats))
	for i := range chats {
		if chats[i].CompanyID != companyID {
			loggerCtx.Warn("Skipping chat in bulk upsert due to mismatched CompanyID",
				zap.String("chat_id", chats[i].ChatID),
				zap.String("chat_company_id", chats[i].CompanyID),
				zap.String("expected_company_id", companyID))
			continue
		}
		chats[i].UpdatedAt = utils.Now() // Ensure UpdatedAt is set
		validChats = append(validChats, chats[i])
	}

	if len(validChats) == 0 {
		loggerCtx.Info("No valid chats found for bulk upsert after tenant ID filtering")
		return nil
	}

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

		// Use chat_id and company_id as conflict target
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "chat_id"}},
			DoUpdates: clause.AssignmentColumns(model.ChatUpdatableFields()), // Use explicitly defined columns
		}).Create(&validChats)

		if result.Error != nil {
			txErr = fmt.Errorf("%w: bulk upsert chats failed: %w", apperrors.ErrDatabase, result.Error)
			return txErr
		}

		if commitErr := tx.Commit().Error; commitErr != nil {
			txErr = checkConstraintViolation(commitErr)
			return txErr
		}
		loggerCtx.Info("Bulk upsert chats successful", zap.Int("chats_processed", len(validChats)), zap.Int64("rows_affected", result.RowsAffected))
		return nil
	}

	commitPolicy := newRetryPolicy(ctx, bulkCommitRetryMaxElapsedTime)
	startTime := utils.Now()
	commitErr := retryableOperation(ctx, commitPolicy, "BulkUpsertChats Commit", operation)
	observer.ObserveDbOperationDuration("bulk_upsert", "chat", companyID, time.Since(startTime), commitErr)
	if commitErr != nil {
		loggerCtx.Error("Failed to bulk upsert chats after retries", zap.Error(commitErr))
		return commitErr // Already wrapped
	}
	return nil
}
