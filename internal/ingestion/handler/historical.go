package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/apperrors"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"go.uber.org/zap"
)

// HistoricalHandler processes historical events
type HistoricalHandler struct {
	service HistoricalService
}

// HistoricalService defines the interface for historical event processing
type HistoricalService interface {
	ProcessHistoricalChats(ctx context.Context, chats []model.UpsertChatPayload, metadata *model.LastMetadata) error
	ProcessHistoricalMessages(ctx context.Context, messages []model.UpsertMessagePayload, metadata *model.LastMetadata) error
	ProcessHistoricalContacts(ctx context.Context, contacts []model.UpsertContactPayload, metadata *model.LastMetadata) error
}

// NewHistoricalHandler creates a new historical event handler
func NewHistoricalHandler(service HistoricalService) *HistoricalHandler {
	return &HistoricalHandler{
		service: service,
	}
}

// HandleEvent processes historical events
func (h *HistoricalHandler) HandleEvent(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)
	log.Info("Processing historical event", zap.String("type", string(eventType)))

	lastMetadata := metadata.ToLastMetadata()
	var err error

	switch eventType {
	case model.V1HistoricalChats:
		err = h.handleHistoricalChats(ctx, lastMetadata, rawEvent)
	case model.V1HistoricalMessages:
		err = h.handleHistoricalMessages(ctx, lastMetadata, rawEvent)
	case model.V1HistoricalContacts:
		err = h.handleHistoricalContacts(ctx, lastMetadata, rawEvent)
	default:
		unsupportedErr := fmt.Errorf("unsupported historical event type: %s", eventType)
		log.Error("Unsupported historical event type", zap.String("eventType", string(eventType)))
		err = apperrors.NewFatal(unsupportedErr, "unsupported historical event type")
	}

	// Return the error (already wrapped by handlers or service layer)
	return err
}

// handleHistoricalChats processes historical chat events
func (h *HistoricalHandler) handleHistoricalChats(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Parse the rawEvent payload as HistoryChat
	var historyChat model.HistoryChatPayload
	if err := json.Unmarshal(rawEvent, &historyChat); err != nil {
		log.Error("Failed to unmarshal historical chats payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal historical chats payload")
	}

	if len(historyChat.Chats) == 0 {
		log.Warn("No chats in history_chats payload")
		return nil
	}

	log.Info("Processing historical chats", zap.Int("count", len(historyChat.Chats)))
	// Return error directly from service (already wrapped)
	return h.service.ProcessHistoricalChats(ctx, historyChat.Chats, metadata)
}

// handleHistoricalMessages processes historical message events
func (h *HistoricalHandler) handleHistoricalMessages(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)
	// Parse the rawEvent payload as HistoryMessage
	var historyMessage model.HistoryMessagePayload
	if err := json.Unmarshal(rawEvent, &historyMessage); err != nil {
		log.Error("Failed to unmarshal historical messages payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal historical messages payload")
	}

	if len(historyMessage.Messages) == 0 {
		log.Warn("No messages in history_messages payload")
		return nil
	}

	log.Info("Processing historical messages", zap.Int("count", len(historyMessage.Messages)))
	// Return error directly from service (already wrapped)
	return h.service.ProcessHistoricalMessages(ctx, historyMessage.Messages, metadata)
}

// handleHistoricalContacts processes historical contact events
func (h *HistoricalHandler) handleHistoricalContacts(ctx context.Context, metadata *model.LastMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)
	// Parse the rawEvent payload
	var contacts model.HistoryContactPayload
	// First try to unmarshal as a direct array of contacts
	if err := json.Unmarshal(rawEvent, &contacts); err != nil {
		log.Error("Failed to unmarshal historical contacts payload", zap.Error(err))
		// Wrap unmarshal error as Fatal
		return apperrors.NewFatal(err, "failed to unmarshal historical contacts payload")
	}

	if len(contacts.Contacts) == 0 {
		log.Warn("No contacts in history_contacts payload")
		return nil
	}

	// Set tenant ID from metadata (already in context by router, service layer will use it)
	log.Info("Processing historical contacts", zap.Int("count", len(contacts.Contacts)))
	// Return error directly from service (already wrapped)
	return h.service.ProcessHistoricalContacts(ctx, contacts.Contacts, metadata)
}
