package ingestion

import (
	"context"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/tenant"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/logger"
	"gitlab.com/timkado/api/daisi-wa-events-processor/pkg/utils"
	"go.uber.org/zap"
)

// EventHandler defines a function that processes events
type EventHandler func(ctx context.Context, eventType model.EventType, metadata *model.MessageMetadata, rawEvent []byte) error

// Router routes events to the appropriate handler based on event type
type Router struct {
	// Map of event type to handler
	handlers map[model.EventType]EventHandler
	// Map of event base type (without version) to handler
	baseHandlers map[model.EventType]EventHandler
	// Default handler for unknown event types
	defaultHandler EventHandler
}

// NewRouter creates a new event router
func NewRouter() *Router {
	return &Router{
		handlers: make(map[model.EventType]EventHandler),
	}
}

// Register registers a handler for an event type
// This handles the exact event type match (including version if present)
func (r *Router) Register(eventType model.EventType, handler EventHandler) {
	r.handlers[eventType] = handler
}

// RegisterDefault registers a default handler for unknown event types
func (r *Router) RegisterDefault(handler EventHandler) {
	r.defaultHandler = handler
}

// Route routes an event to the appropriate handler
func (r *Router) Route(ctx context.Context, metadata *model.MessageMetadata, rawEvent []byte) error {
	log := logger.FromContext(ctx)

	// Add event metadata to the log context
	log = log.With(
		zap.String("event_type", metadata.MessageSubject),
		zap.String("event_id", metadata.MessageID),
		zap.String("company_id", metadata.CompanyID),
	)
	ctx = logger.WithLogger(ctx, log)

	// Add tenant to context
	if metadata.CompanyID != "" {
		ctx = tenant.WithCompanyID(ctx, metadata.CompanyID)
	}

	// Get event type from metadata
	eventType, found := model.MapToBaseEventType(metadata.MessageSubject)
	if !found {
		// Log the error but don't return yet, let the default handler logic proceed.
		log.Warn("Could not map subject to a known base event type", zap.String("subject", metadata.MessageSubject))
		// eventType will be empty ("" model.EventType), which is handled below.
	}

	// Log event receipt
	log.Info("Event received",
		zap.String("payload_size", utils.ByteCountSI(len(rawEvent))),
		zap.String("version", eventType.GetVersion()),
		zap.String("base_type", string(eventType.GetBaseType())),
	)

	// Try exact match first
	handler, ok := r.handlers[eventType]

	// Use default handler if no specific handler found
	if !ok && r.defaultHandler != nil {
		log.Warn("No specific handler for event type, using default")
		return r.defaultHandler(ctx, eventType, metadata, rawEvent)
	} else if !ok {
		log.Error("No handler registered for event type")
		return nil // or return an error if preferred
	}

	// Handle the event
	return handler(ctx, eventType, metadata, rawEvent)
}
