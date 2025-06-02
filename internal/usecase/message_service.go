package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sourcegraph/conc/iter"
	"go.uber.org/zap"
	"gorm.io/datatypes"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/validator"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// --- Helper Function for Repository Error Handling ---

// handleRepositoryError maps standard apperrors from the repository layer
// to FatalError or RetryableError for the use case layer.
func handleRepositoryError(ctx context.Context, err error, operation string, messageID string) error {
	if err == nil {
		return nil
	}

	// Extract logger from context
	log := logger.FromContext(ctx)

	logFields := []zap.Field{
		zap.String("operation", operation),
		zap.Error(err),
	}
	if messageID != "" {
		logFields = append(logFields, zap.String("message_id", messageID))
	}

	// Specific fatal errors (cannot be resolved by retry)
	if errors.Is(err, apperrors.ErrNotFound) {
		log.Warn("Repository operation failed: Not found", logFields...)
		return apperrors.NewFatal(err, "%s failed: resource not found", operation)
	}
	if errors.Is(err, apperrors.ErrDuplicate) {
		log.Warn("Repository operation failed: Duplicate resource", logFields...)
		return apperrors.NewFatal(err, "%s failed: duplicate resource", operation)
	}
	if errors.Is(err, apperrors.ErrBadRequest) {
		log.Warn("Repository operation failed: Bad request", logFields...)
		return apperrors.NewFatal(err, "%s failed: bad request data", operation)
	}
	if errors.Is(err, apperrors.ErrUnauthorized) {
		log.Error("Repository operation failed: Unauthorized", logFields...) // Error level for auth issues
		return apperrors.NewFatal(err, "%s failed: unauthorized", operation)
	}
	if errors.Is(err, apperrors.ErrConflict) {
		log.Warn("Repository operation failed: Conflict", logFields...)
		return apperrors.NewFatal(err, "%s failed: resource conflict", operation)
	}

	// General database errors (potentially retryable)
	if errors.Is(err, apperrors.ErrDatabase) {
		// Log as error because it indicates a DB issue that might need attention
		log.Error("Repository operation failed: Database error", logFields...)
		// Decide if retryable at this level. Let's assume DB errors are potentially transient.
		return apperrors.NewRetryable(err, "%s failed: database error", operation)
	}

	// Timeout errors (retryable)
	if errors.Is(err, apperrors.ErrTimeout) {
		log.Warn("Repository operation failed: Timeout", logFields...)
		return apperrors.NewRetryable(err, "%s failed: operation timeout", operation)
	}

	// NATS errors (potentially retryable)
	if errors.Is(err, apperrors.ErrNATS) {
		log.Error("Repository operation failed: NATS error", logFields...)
		return apperrors.NewRetryable(err, "%s failed: NATS communication error", operation)
	}

	// --- Default Handling ---
	// Wrap other unexpected errors as fatal by default.
	log.Error("Repository operation failed: Unexpected error", logFields...)
	return apperrors.NewFatal(err, "%s failed: unexpected repository error", operation)
}

// ProcessHistoricalMessages processes historical messages in bulk
func (s *EventService) ProcessHistoricalMessages(ctx context.Context, messages []model.UpsertMessagePayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)
	start := utils.Now()

	// Validate input
	if len(messages) == 0 {
		log.Warn("No messages to process in historical messages payload")
		return nil
	}

	// Extract tenant ID
	companyID, err := tenant.FromContext(ctx)
	if err != nil || companyID == "" {
		log.Error("Failed to get tenant ID from context",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// Validate each message payload
	for i, message := range messages {
		if err := validator.Validate(message); err != nil {
			log.Error("Validation failed for message",
				zap.String("message_id", message.MessageID),
				zap.Int("index", i),
				zap.Error(err),
			)
			return apperrors.NewFatal(err, "validation error at index %d", i)
		}

		// Validate that cluster matches tenant ID
		if err := validateClusterTenant(ctx, message.CompanyID); err != nil {
			log.Error("CompanyID validation failed for message",
				zap.String("message_id", message.MessageID),
				zap.String("company_id", message.CompanyID),
				zap.String("context_company_id", companyID),
				zap.Int("index", i),
				zap.Error(err),
			)
			return apperrors.NewFatal(err, "company validation error at index %d", i)
		}
	}

	// Convert metadata to datatypes.JSON for storage
	var metadataJSON datatypes.JSON
	if metadata != nil {
		metadataMap := map[string]interface{}{
			"consumer_sequence": metadata.ConsumerSequence,
			"stream_sequence":   metadata.StreamSequence,
			"stream":            metadata.Stream,
			"consumer":          metadata.Consumer,
			"domain":            metadata.Domain,
			"message_id":        metadata.MessageID,
			"message_subject":   metadata.MessageSubject,
			"processed_at":      utils.Now(),
		}
		metadataJSON = utils.MustMarshalJSON(metadataMap)
	}

	// Transform payloads to database models
	dbMessages := make([]model.Message, 0, len(messages))
	for _, msg := range messages {
		if msg.MessageID == "" {
			log.Warn("Skipping message with empty ID")
			continue
		}

		dbMessage := model.Message{
			MessageID:        msg.MessageID,
			Jid:              msg.Jid,
			ToUser:           msg.ToUser,
			FromUser:         msg.FromUser,
			Flow:             msg.Flow,
			Type:             msg.Type,
			Status:           msg.Status,
			MessageTimestamp: msg.MessageTimestamp,
			LastMetadata:     metadataJSON,
			CompanyID:        msg.CompanyID,
			AgentID:          msg.AgentID,
			ChatID:           msg.ChatID,
		}

		// Convert MessageObj to datatypes.JSON
		if msg.MessageObj != nil {
			dbMessage.MessageObj = utils.MustMarshalJSON(msg.MessageObj)
		}

		// Transform Key if present
		if msg.Key != nil {
			if err := validator.Validate(msg.Key); err != nil {
				log.Error("Key validation failed",
					zap.String("message_id", msg.MessageID),
					zap.Error(err),
				)
				return apperrors.NewFatal(err, "key validation error for message %s", msg.MessageID)
			}

			// Convert KeyPayload to datatypes.JSON
			keyMap := map[string]interface{}{
				"id":        msg.Key.ID,
				"fromMe":    msg.Key.FromMe,
				"remoteJid": msg.Key.RemoteJid,
			}
			dbMessage.Key = utils.MustMarshalJSON(keyMap)
		}

		// Set EventTimestamp from MessageTimestamp if available
		if msg.MessageTimestamp > 0 {
			dbMessage.MessageDate = model.CreateTimeFromTimestamp(msg.MessageTimestamp)
		} else {
			dbMessage.MessageDate = utils.Now()
		}

		// Set other timestamps
		if dbMessage.CreatedAt.IsZero() {
			dbMessage.CreatedAt = utils.Now()
		}
		dbMessage.UpdatedAt = utils.Now()

		dbMessages = append(dbMessages, dbMessage)
	}

	// Perform bulk operation
	if err := s.messageRepo.BulkUpsert(ctx, dbMessages); err != nil {
		log.Error("Failed to process historical messages (BulkUpsert)",
			zap.Int("count", len(dbMessages)),
			zap.Error(err),
		)
		// Handle repository error
		return handleRepositoryError(ctx, err, "BulkUpsertHistoricalMessages", "") // Pass ctx
	}

	// --- Post-processing: Submit onboarding tasks if needed (concurrently) ---
	iter.ForEach(dbMessages, func(dbMsg *model.Message) {
		// Submit task to onboarding worker pool
		if submitErr := s.submitOnboardingTaskIfNeeded(ctx, *dbMsg, metadataJSON); submitErr != nil {
			// Log the submission error, but don't fail the main operation
			logger.FromContext(ctx).Warn("Failed to submit onboarding task for historical message",
				zap.String("message_id", dbMsg.MessageID),
				zap.Error(submitErr),
			)
		}
	})
	// --- End Post-processing ---

	log.Info("Successfully processed historical messages",
		zap.Int("count", len(dbMessages)),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

// UpsertMessage processes the upsertion of a single message.
func (s *EventService) UpsertMessage(ctx context.Context, payload model.UpsertMessagePayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)

	// Validate input
	if err := validator.Validate(payload); err != nil {
		log.Error("Message validation failed",
			zap.String("message_id", payload.MessageID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "message validation failed")
	}

	// Extract tenant ID
	companyID, err := tenant.FromContext(ctx)
	if err != nil || companyID == "" {
		log.Error("Failed to get tenant ID from context",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// Validate that company_id matches tenant ID
	if err := validateClusterTenant(ctx, payload.CompanyID); err != nil {
		log.Error("CompanyID validation failed for message",
			zap.String("message_id", payload.MessageID),
			zap.String("company_id", payload.CompanyID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "company_id validation error")
	}

	// Convert metadata to datatypes.JSON for storage
	var metadataJSON datatypes.JSON
	if metadata != nil {
		metadataMap := map[string]interface{}{
			"consumer_sequence": metadata.ConsumerSequence,
			"stream_sequence":   metadata.StreamSequence,
			"stream":            metadata.Stream,
			"consumer":          metadata.Consumer,
			"domain":            metadata.Domain,
			"message_id":        metadata.MessageID,
			"message_subject":   metadata.MessageSubject,
			"processed_at":      utils.Now(),
		}
		metadataJSON = utils.MustMarshalJSON(metadataMap)
	}

	// Transform to database model
	message := model.Message{
		MessageID:        payload.MessageID,
		Jid:              payload.Jid,
		ToUser:           payload.ToUser,
		FromUser:         payload.FromUser,
		Flow:             payload.Flow,
		Type:             payload.Type,
		Status:           payload.Status,
		MessageTimestamp: payload.MessageTimestamp,
		LastMetadata:     metadataJSON,
		CompanyID:        payload.CompanyID,
		AgentID:          payload.AgentID,
		ChatID:           payload.ChatID,
	}

	// Convert MessageObj to datatypes.JSON
	if payload.MessageObj != nil {
		message.MessageObj = utils.MustMarshalJSON(payload.MessageObj)
	}

	// Transform Key if present
	if payload.Key != nil {
		if err := validator.Validate(payload.Key); err != nil {
			log.Error("Key validation failed",
				zap.String("message_id", payload.MessageID),
				zap.Error(err),
			)
			return apperrors.NewFatal(err, "key validation error")
		}

		// Convert KeyPayload to datatypes.JSON
		keyMap := map[string]interface{}{
			"id":        payload.Key.ID,
			"fromMe":    payload.Key.FromMe,
			"remoteJid": payload.Key.RemoteJid,
		}
		message.Key = utils.MustMarshalJSON(keyMap)
	}

	// Set EventTimestamp from MessageTimestamp if available
	if payload.MessageTimestamp > 0 {
		message.MessageDate = model.CreateTimeFromTimestamp(payload.MessageTimestamp)
	} else {
		message.MessageDate = utils.Now()
	}

	// Set timestamps
	if message.CreatedAt.IsZero() {
		message.CreatedAt = utils.Now()
	}
	message.UpdatedAt = utils.Now()

	// Save to repo
	if err := s.messageRepo.Save(ctx, message); err != nil {
		log.Error("Failed to upsert message (Save)",
			zap.String("message_id", message.MessageID),
			zap.Error(err),
		)
		// Handle repository error
		return handleRepositoryError(ctx, err, "SaveMessage", message.MessageID) // Pass ctx
	}

	// --- Post-processing: Submit onboarding task if needed ---
	// Call the helper function to submit the task, log any submission error but don't fail the upsert
	if submitErr := s.submitOnboardingTaskIfNeeded(ctx, message, metadataJSON); submitErr != nil {
		log.Warn("Failed to submit onboarding task after message upsert",
			zap.String("message_id", message.MessageID),
			zap.Error(submitErr),
		)
	}
	// --- End Post-processing ---

	log.Info("Successfully upserted message",
		zap.String("message_id", message.MessageID),
		zap.String("chat_id", message.Jid),
	)
	return nil
}

// UpdateMessage processes the update of a single message.
func (s *EventService) UpdateMessage(ctx context.Context, payload model.UpdateMessagePayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)

	// Validate input
	if err := validator.Validate(payload); err != nil {
		log.Error("Message update validation failed",
			zap.String("message_id", payload.MessageID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "message update validation failed")
	}

	// Extract tenant ID
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		log.Error("Failed to get tenant ID from context",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// First, get the existing message to update only the changed fields
	existingMessage, err := s.messageRepo.FindByMessageID(ctx, payload.MessageID)
	if err != nil {
		// Handle repository error for FindByMessageID
		// No need to log here, handleRepositoryError does it
		return handleRepositoryError(ctx, err, "FindMessageByIDForUpdate", payload.MessageID) // Pass ctx
	}

	if existingMessage == nil {
		log.Warn("Message not found for update",
			zap.String("message_id", payload.MessageID),
		)
		return errors.New("message not found")
	}

	// Convert metadata to datatypes.JSON for storage
	var metadataJSON datatypes.JSON
	if metadata != nil {
		metadataMap := map[string]interface{}{
			"consumer_sequence": metadata.ConsumerSequence,
			"stream_sequence":   metadata.StreamSequence,
			"stream":            metadata.Stream,
			"consumer":          metadata.Consumer,
			"domain":            metadata.Domain,
			"message_id":        metadata.MessageID,
			"message_subject":   metadata.MessageSubject,
			"processed_at":      utils.Now(),
		}
		metadataJSON = utils.MustMarshalJSON(metadataMap)
	}

	// Validate CompanyID matches payload and context
	if payload.CompanyID != existingMessage.CompanyID {
		log.Warn("Company ID mismatch during update (payload vs existing)",
			zap.String("message_id", payload.MessageID),
			zap.String("payload_company_id", payload.CompanyID),
			zap.String("existing_company_id", existingMessage.CompanyID),
		)
		mismatchErr := fmt.Errorf("payload company id %s does not match existing %s", payload.CompanyID, existingMessage.CompanyID)
		return apperrors.NewFatal(mismatchErr, "company ID mismatch for message %s", payload.MessageID)
	}

	// Validate Company ID consistency (from context vs existing)
	if companyID != existingMessage.CompanyID {
		log.Warn("Company ID mismatch during update (context vs existing)",
			zap.String("message_id", payload.MessageID),
			zap.String("context_company_id", companyID),
			zap.String("existing_company_id", existingMessage.CompanyID),
		)
		mismatchErr := fmt.Errorf("context company id %s does not match existing %s", companyID, existingMessage.CompanyID)
		return apperrors.NewFatal(mismatchErr, "context company ID mismatch for message %s", payload.MessageID)
	}

	// Update only the changed fields
	message := *existingMessage
	message.LastMetadata = metadataJSON
	message.UpdatedAt = utils.Now()

	if payload.Status != "" {
		message.Status = payload.Status
	}

	if payload.EditedMessageObj != nil {
		message.EditedMessageObj = utils.MustMarshalJSON(payload.EditedMessageObj)
	}

	if payload.IsDeleted != message.IsDeleted {
		message.IsDeleted = payload.IsDeleted
	}

	// Update in repo
	if err := s.messageRepo.Update(ctx, message); err != nil {
		log.Error("Failed to update message (Update)",
			zap.String("message_id", message.MessageID),
			zap.Error(err),
		)
		// Handle repository error for Update
		return handleRepositoryError(ctx, err, "UpdateMessage", message.MessageID) // Pass ctx
	}

	log.Info("Successfully updated message",
		zap.String("message_id", message.MessageID),
	)
	return nil
}

// --- Helper Function for Onboarding Log Creation ---

// submitOnboardingTaskIfNeeded checks if an incoming message necessitates creating an onboarding log
// entry and submits the task to the worker pool if required.
// It performs preliminary checks (flow type, 'from' field) before submission.
// Returns an error only if there was an issue submitting the task to the pool.
func (s *EventService) submitOnboardingTaskIfNeeded(ctx context.Context, message model.Message, metadataJSON datatypes.JSON) error {
	log := logger.FromContext(ctx).With(zap.String("checked_message_id", message.MessageID))

	// 1. Perform quick checks: only submit if it's an incoming message with a 'From' field.
	if message.Flow != model.MessageFlowIncoming || message.FromUser == "" {
		log.Debug("Skipping onboarding task submission: not an applicable message type")
		return nil
	}

	// Prepare task data
	// Create a new detached context for the background task
	taskCtx := context.Background()           // Use a fresh background context
	taskCtx = logger.WithLogger(taskCtx, log) // Carry logger forward

	taskData := OnboardingTaskData{
		Ctx:          taskCtx, // Pass the detached context
		Message:      message,
		MetadataJSON: metadataJSON,
	}

	// 2. Submit to the worker pool
	if err := s.onboardingWorker.SubmitTask(taskData); err != nil {
		log.Error("Failed to submit onboarding task to worker pool", zap.Error(err))
		// Return the submission error so the caller can decide how to handle (e.g., log it)
		return fmt.Errorf("failed to submit onboarding task for message %s: %w", message.MessageID, err)
	}

	log.Debug("Successfully submitted onboarding task to worker pool")
	return nil
}
