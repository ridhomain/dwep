package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap"
)

// RealtimeHandler processes realtime events
type RealtimeHandler struct {
	service RealtimeService
}

// RealtimeService defines the interface for realtime event processing
type RealtimeService interface {
	UpsertChat(ctx context.Context, chat model.UpsertChatPayload, metadata *model.LastMetadata) error
	UpdateChat(ctx context.Context, chat model.UpdateChatPayload, metadata *model.LastMetadata) error
	UpsertMessage(ctx context.Context, message model.UpsertMessagePayload, metadata *model.LastMetadata) error
	UpdateMessage(ctx context.Context, message model.UpdateMessagePayload, metadata *model.LastMetadata) error
	UpsertContact(ctx context.Context, contact model.UpsertContactPayload, metadata *model.LastMetadata) error
	UpdateContact(ctx context.Context, contact model.UpdateContactPayload, metadata *model.LastMetadata) error
	UpsertAgent(ctx context.Context, contact model.UpsertAgentPayload, metadata *model.LastMetadata) error
}

// NewRealtimeHandler creates a new realtime event handler
func NewRealtimeHandler(service RealtimeService) *RealtimeHandler {
	return &RealtimeHandler{
		service: service,
	}
}

// HandleEvent processes realtime events
func (h *RealtimeHandler) HandleEvent(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
	// Generate request ID
	requestID := uuid.NewString()
	// Add request ID to context
	ctx = tenant.WithRequestID(ctx, requestID)

	log := logger.FromContext(ctx)
	log.Info("Processing realtime event", zap.String("type", string(eventType)))

	lastMetadata := metadata.ToLastMetadata()
	var err error
	switch eventType {
	case model.V1ChatsUpsert:
		err = h.handleChatUpsert(ctx, lastMetadata, rawEvent)
	case model.V1ChatsUpdate:
		err = h.handleChatUpdate(ctx, lastMetadata, rawEvent)
	case model.V1MessagesUpsert:
		err = h.handleMessageUpsert(ctx, lastMetadata, rawEvent)
	case model.V1MessagesUpdate:
		err = h.handleMessageUpdate(ctx, lastMetadata, rawEvent)
	case model.V1ContactsUpsert:
		err = h.handleContactUpsert(ctx, lastMetadata, rawEvent)
	case model.V1ContactsUpdate:
		err = h.handleContactUpdate(ctx, lastMetadata, rawEvent)
	case model.V1Agents:
		err = h.handleAgentEvent(ctx, lastMetadata, metadata.MessageSubject, rawEvent)
	default:
		unsupportedErr := fmt.Errorf("unsupported realtime event type: %s", eventType)
		log.Error("Unsupported realtime event type", zap.String("eventType", string(eventType)))
		err = apperrors.NewFatal(unsupportedErr, "unsupported realtime event type")
	}
	return err // Return error (already wrapped by handlers or service)
}

// handleChatUpsert processes chat upsert events
func (h *RealtimeHandler) handleChatUpsert(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Parse the rawEvent payload
	var chat model.UpsertChatPayload
	if err := json.Unmarshal(rawEvent, &chat); err != nil {
		log.Error("Failed to unmarshal chat upsert payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal chat upsert payload")
	}

	// Enrich payload with CompanyID from metadata
	if chat.CompanyID == "" {
		chat.CompanyID = metadata.CompanyID
	}

	log.Info("Processing chat upsert", zap.String("chat_id", chat.ChatID))
	// Return error directly from service (already wrapped)
	return h.service.UpsertChat(ctx, chat, metadata)
}

// handleChatUpdate processes chat update events
func (h *RealtimeHandler) handleChatUpdate(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Parse the rawEvent payload
	var chat model.UpdateChatPayload
	if err := json.Unmarshal(rawEvent, &chat); err != nil {
		log.Error("Failed to unmarshal chat update payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal chat update payload")
	}

	// Basic validation
	if chat.ChatID == "" {
		validationErr := fmt.Errorf("chat ID is required for update")
		log.Error("Chat update validation failed", zap.Error(validationErr))
		// Wrap validation error as Fatal
		return apperrors.NewFatal(validationErr, "chat ID is required for update")
	}

	// Enrich payload with CompanyID from metadata
	if chat.CompanyID == "" {
		chat.CompanyID = metadata.CompanyID
	}

	log.Info("Processing chat update", zap.String("chat_id", chat.ChatID))
	// Return error directly from service (already wrapped)
	return h.service.UpdateChat(ctx, chat, metadata)
}

// handleMessageUpsert processes message upsert events
func (h *RealtimeHandler) handleMessageUpsert(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Parse the rawEvent payload
	var payload model.UpsertMessagePayload
	if err := json.Unmarshal(rawEvent, &payload); err != nil {
		log.Error("Failed to unmarshal message upsert payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal message upsert payload")
	}

	// Enrich payload with CompanyID from metadata
	if payload.CompanyID == "" {
		payload.CompanyID = metadata.CompanyID
	}

	log.Info("Processing message upsert",
		zap.String("message_id", payload.MessageID),
		zap.String("nats_message_id", metadata.MessageID))
	// Return error directly from service (already wrapped)
	return h.service.UpsertMessage(ctx, payload, metadata)
}

// handleMessageUpdate processes message update events
func (h *RealtimeHandler) handleMessageUpdate(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Parse the rawEvent payload
	var payload model.UpdateMessagePayload
	if err := json.Unmarshal(rawEvent, &payload); err != nil {
		log.Error("Failed to unmarshal message update payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal message update payload")
	}
	// Basic validation
	if payload.MessageID == "" {
		validationErr := fmt.Errorf("message ID is required for update")
		log.Error("Message update validation failed", zap.Error(validationErr))
		// Wrap validation error as Fatal
		return apperrors.NewFatal(validationErr, "message ID is required for update")
	}

	// Enrich payload with CompanyID from metadata
	if payload.CompanyID == "" {
		payload.CompanyID = metadata.CompanyID
	}

	log.Info("Processing message update",
		zap.String("message_id", payload.MessageID),
		zap.String("nats_message_id", metadata.MessageID))
	// Return error directly from service (already wrapped)
	return h.service.UpdateMessage(ctx, payload, metadata)
}

// handleContactUpsert processes contact upsert events
func (h *RealtimeHandler) handleContactUpsert(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Parse the rawEvent payload
	var payload model.UpsertContactPayload
	if err := json.Unmarshal(rawEvent, &payload); err != nil {
		log.Error("Failed to unmarshal contact upsert payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal contact upsert payload")
	}

	// Enrich payload with CompanyID from metadata
	if payload.CompanyID == "" {
		payload.CompanyID = metadata.CompanyID
	}

	log.Info("Processing contact upsert", zap.String("phone_number", payload.PhoneNumber), zap.String("agent_id", payload.AgentID))
	// Return error directly from service (already wrapped)
	return h.service.UpsertContact(ctx, payload, metadata)
}

// handleContactUpdate processes contact update events
func (h *RealtimeHandler) handleContactUpdate(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Parse the rawEvent payload
	var payload model.UpdateContactPayload
	if err := json.Unmarshal(rawEvent, &payload); err != nil {
		log.Error("Failed to unmarshal contact update payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal contact update payload")
	}

	// Enrich payload with CompanyID from metadata
	if payload.CompanyID == "" {
		payload.CompanyID = metadata.CompanyID
	}

	log.Info("Processing contact update", zap.String("phone_number", payload.PhoneNumber), zap.String("agent_id", payload.AgentID))
	// Return error directly from service (already wrapped)
	return h.service.UpdateContact(ctx, payload, metadata)
}

// handleAgentEvent processes agent-related events (e.g., v1.connection.update)
func (h *RealtimeHandler) handleAgentEvent(ctx context.Context, metadata *model.LastMetadata, subject string, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Parse the agent payload
	var payload model.UpsertAgentPayload
	if err := json.Unmarshal(rawEvent, &payload); err != nil {
		log.Error("Failed to unmarshal agent event payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal agent payload")
	}

	// Enrich payload with CompanyID from metadata
	if payload.CompanyID == "" {
		payload.CompanyID = metadata.CompanyID
	}

	log.Info("Processing agent event", zap.String("agent_id", payload.AgentID))
	// Return error directly from service (already wrapped)
	return h.service.UpsertAgent(ctx, payload, metadata)
}
