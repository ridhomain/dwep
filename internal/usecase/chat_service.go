package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	apperrors "gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"go.uber.org/zap"
	"gorm.io/datatypes"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/validator"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
)

// ProcessHistoricalChats processes historical chats in bulk
func (s *EventService) ProcessHistoricalChats(ctx context.Context, chats []model.UpsertChatPayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)
	start := utils.Now()

	// Validate input
	if len(chats) == 0 {
		log.Warn("No chats to process in historical chats payload")
		return nil
	}

	// Extract tenant ID FIRST
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		log.Error("Failed to get tenant ID from context for historical chats",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// Validate each chat payload
	for i, chat := range chats {
		if err := validator.Validate(chat); err != nil {
			log.Error("Validation failed for historical chat",
				zap.String("chat_id", chat.ChatID),
				zap.Int("index", i),
				zap.Error(err),
			)
			// Validation errors are fatal
			return apperrors.NewFatal(err, "validation failed for historical chat at index %d", i)
		}

		// Validate that chat's CompanyID matches tenant ID from context
		if chat.CompanyID != companyID {
			log.Error("CompanyID mismatch for historical chat",
				zap.String("chat_id", chat.ChatID),
				zap.String("payload_company_id", chat.CompanyID),
				zap.String("context_company_id", companyID),
				zap.Int("index", i),
			)
			// CompanyID mismatch is fatal
			err := fmt.Errorf("payload company ID %s does not match context tenant ID %s", chat.CompanyID, companyID)
			return apperrors.NewFatal(err, "company ID mismatch for historical chat at index %d", i)
		}
	}

	// Convert metadata to map for storage
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
	dbChats := make([]model.Chat, 0, len(chats))
	for _, chat := range chats {
		if chat.ChatID == "" {
			log.Warn("Skipping chat with empty ID")
			continue
		}

		dbChat := model.Chat{
			ChatID:                chat.ChatID,
			Jid:                   chat.Jid,
			PhoneNumber:           chat.PhoneNumber,
			PushName:              chat.PushName,
			GroupName:             chat.GroupName,
			ConversationTimestamp: chat.ConversationTimestamp,
			UnreadCount:           chat.UnreadCount,
			IsGroup:               chat.IsGroup,
			NotSpam:               chat.NotSpam,
			CompanyID:             chat.CompanyID,
			AgentID:               chat.AgentID,
		}

		// Convert LastMessageObj to datatypes.JSON
		if chat.LastMessageObj != nil {
			dbChat.LastMessageObj = utils.MustMarshalJSON(chat.LastMessageObj)
		}

		// Set LastMetadata
		dbChat.LastMetadata = metadataJSON

		// Set timestamps if not provided
		if dbChat.CreatedAt.IsZero() {
			dbChat.CreatedAt = utils.Now()
		}
		dbChat.UpdatedAt = utils.Now()

		dbChats = append(dbChats, dbChat)
	}

	// Perform bulk operation
	if err := s.chatRepo.BulkUpsert(ctx, dbChats); err != nil {
		logFields := []zap.Field{
			zap.Int("count", len(dbChats)),
			zap.Error(err),
		}
		// Check if the error is retryable
		if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
			log.Warn("Potentially retryable error during historical chats bulk upsert", logFields...)
			return apperrors.NewRetryable(err, "retryable repository error processing historical chats")
		} else {
			log.Error("Fatal error during historical chats bulk upsert", logFields...)
			return apperrors.NewFatal(err, "fatal repository error processing historical chats")
		}
	}

	log.Info("Successfully processed historical chats",
		zap.Int("count", len(dbChats)),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

// UpsertChat creates or updates a chat
func (s *EventService) UpsertChat(ctx context.Context, payload model.UpsertChatPayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)

	// Extract tenant ID FIRST
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		log.Error("Failed to get tenant ID from context for upsert chat",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// Validate input
	if err := validator.Validate(payload); err != nil {
		log.Error("Chat validation failed",
			zap.String("chat_id", payload.ChatID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "chat validation failed")
	}

	// Validate that cluster matches tenant ID
	if payload.CompanyID != companyID {
		log.Error("CompanyID mismatch for upsert chat",
			zap.String("chat_id", payload.ChatID),
			zap.String("payload_company_id", payload.CompanyID),
			zap.String("context_company_id", companyID),
		)
		err := fmt.Errorf("payload company ID %s does not match context tenant ID %s", payload.CompanyID, companyID)
		return apperrors.NewFatal(err, "chat company ID mismatch")
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
	chat := model.Chat{
		ChatID:                payload.ChatID,
		Jid:                   payload.Jid,
		PhoneNumber:           payload.PhoneNumber,
		PushName:              payload.PushName,
		GroupName:             payload.GroupName,
		ConversationTimestamp: payload.ConversationTimestamp,
		UnreadCount:           payload.UnreadCount,
		IsGroup:               payload.IsGroup,
		NotSpam:               payload.NotSpam,
		CompanyID:             payload.CompanyID,
		AgentID:               payload.AgentID,
	}

	// Convert LastMessageObj to datatypes.JSON
	if payload.LastMessageObj != nil {
		chat.LastMessageObj = utils.MustMarshalJSON(payload.LastMessageObj)
	}

	// Set LastMetadata
	chat.LastMetadata = metadataJSON

	// Set timestamps
	if chat.CreatedAt.IsZero() {
		chat.CreatedAt = utils.Now()
	}
	chat.UpdatedAt = utils.Now()

	// Save to repo
	if err := s.chatRepo.Save(ctx, chat); err != nil {
		logFields := []zap.Field{
			zap.String("chat_id", chat.ChatID),
			zap.Error(err),
		}
		// Check if the error is retryable
		if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
			log.Warn("Potentially retryable error during chat upsert", logFields...)
			return apperrors.NewRetryable(err, "retryable repository error during chat upsert")
		} else {
			log.Error("Fatal error during chat upsert", logFields...)
			return apperrors.NewFatal(err, "fatal repository error during chat upsert")
		}
	}

	log.Info("Successfully upserted chat", zap.String("chat_id", chat.ChatID))
	return nil
}

// UpdateChat updates an existing chat
func (s *EventService) UpdateChat(ctx context.Context, payload model.UpdateChatPayload, metadata *model.LastMetadata) error {
	log := logger.FromContext(ctx)

	// Validate input
	if err := validator.Validate(payload); err != nil {
		log.Error("Chat update validation failed",
			zap.String("chat_id", payload.ChatID),
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "chat update validation failed")
	}

	// Extract tenant ID FIRST
	companyID, err := tenant.FromContext(ctx)
	if err != nil {
		log.Error("Failed to get tenant ID from context",
			zap.Error(err),
		)
		return apperrors.NewFatal(err, "failed to get tenant ID from context")
	}

	// First, get the existing chat to update only the changed fields
	existingChat, err := s.chatRepo.FindChatByChatID(ctx, payload.ChatID)
	if err != nil {
		logFields := []zap.Field{
			zap.String("chat_id", payload.ChatID),
			zap.Error(err),
		}
		// Decide if FindByChatID error is retryable
		if errors.Is(err, apperrors.ErrNotFound) {
			log.Warn("Chat not found for update", logFields...)
			// Return fatal error immediately if not found
			return apperrors.NewFatal(err, "chat not found for update (chat_id: %s)", payload.ChatID)
		} else if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
			log.Warn("Potentially retryable error fetching chat for update", logFields...)
			return apperrors.NewRetryable(err, "retryable repository error fetching chat for update")
		} else {
			log.Error("Fatal error fetching chat for update", logFields...)
			return apperrors.NewFatal(err, "fatal repository error fetching chat for update")
		}
	}
	// If we reached here, existingChat is guaranteed to be non-nil because ErrNotFound causes early return

	// Validate CompanyID from payload matches existing chat's CompanyID
	if payload.CompanyID != existingChat.CompanyID {
		log.Warn("Company ID mismatch during chat update payload vs existing",
			zap.String("chat_id", payload.ChatID),
			zap.String("payload_company_id", payload.CompanyID),
			zap.String("existing_company_id", existingChat.CompanyID),
		)
		// Treat this as a fatal bad request
		return apperrors.NewFatal(fmt.Errorf("payload company id %s does not match existing %s", payload.CompanyID, existingChat.CompanyID), "company ID mismatch for chat %s", payload.ChatID)
	}

	// Validate Company ID consistency (from context vs existing)
	if companyID != existingChat.CompanyID {
		log.Warn("Company ID (from context) mismatch with existing chat's Company ID",
			zap.String("chat_id", payload.ChatID),
			zap.String("context_company_id", companyID),
			zap.String("existing_company_id", existingChat.CompanyID),
		)
		// This indicates a potential authorization or logic issue, fatal.
		return apperrors.NewFatal(fmt.Errorf("context company id %s does not match existing %s", companyID, existingChat.CompanyID), "context company ID mismatch for chat %s", payload.ChatID)
	}

	// Convert metadata to datatypes.JSON for storage
	var lastMetadataJSON datatypes.JSON
	if metadata != nil {
		lastMetadata := map[string]interface{}{
			"consumer_sequence": metadata.ConsumerSequence,
			"stream_sequence":   metadata.StreamSequence,
			"stream":            metadata.Stream,
			"consumer":          metadata.Consumer,
			"domain":            metadata.Domain,
			"message_id":        metadata.MessageID,
			"message_subject":   metadata.MessageSubject,
			"processed_at":      utils.Now(),
		}
		lastMetadataJSON = utils.MustMarshalJSON(lastMetadata)
	}

	// Update only the changed fields
	chat := *existingChat
	if chat.ConversationTimestamp <= payload.ConversationTimestamp {
		chat.ConversationTimestamp = payload.ConversationTimestamp
	}

	chat.UpdatedAt = utils.Now()
	chat.LastMetadata = lastMetadataJSON

	// Increment UnreadCount if provided in the payload (as per requirement)
	if payload.UnreadCount == 1 {
		// Increment: Add 1 to existing count
		chat.UnreadCount = chat.UnreadCount + 1
	} else {
		// Direct assignment: Set to exact value (including 0 for mark as read)
		chat.UnreadCount = payload.UnreadCount
	}

	if payload.UnreadCount <= 0 {
		chat.UnreadCount = 0
	}

	if payload.LastMessageObj != nil {
		chat.LastMessageObj = utils.MustMarshalJSON(payload.LastMessageObj)
	}

	chat.UpdatedAt = utils.Now()
	chat.LastMetadata = lastMetadataJSON

	// Update in repo
	if err := s.chatRepo.Update(ctx, chat); err != nil {
		logFields := []zap.Field{
			zap.String("chat_id", chat.ChatID),
			zap.Error(err),
		}
		// Check if the error is retryable
		if errors.Is(err, apperrors.ErrDatabase) || errors.Is(err, apperrors.ErrTimeout) || errors.Is(err, apperrors.ErrConflict) {
			log.Warn("Potentially retryable error during chat update", logFields...)
			return apperrors.NewRetryable(err, "retryable repository error during chat update")
		} else {
			log.Error("Fatal error during chat update", logFields...)
			return apperrors.NewFatal(err, "fatal repository error during chat update")
		}
	}
	return nil
}
