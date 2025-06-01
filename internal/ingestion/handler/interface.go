package handler

import (
	"context"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// EventHandlerInterface defines the common interface for event handlers
type EventHandlerInterface interface {
	// HandleEvent processes an event
	HandleEvent(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error
}

// HistoricalHandlerInterface defines the interface for historical event handlers
type HistoricalHandlerInterface interface {
	EventHandlerInterface
}

// RealtimeHandlerInterface defines the interface for realtime event handlers
type RealtimeHandlerInterface interface {
	EventHandlerInterface
}

// Ensure the handlers implement the interfaces
var _ HistoricalHandlerInterface = (*HistoricalHandler)(nil)
var _ RealtimeHandlerInterface = (*RealtimeHandler)(nil)
