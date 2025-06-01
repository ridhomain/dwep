package ingestion

import (
	"context"

	"gitlab.com/timkado/api/daisi-wa-events-processor/internal/model"
)

// RouterInterface defines the interface for an event router
type RouterInterface interface {
	// Register registers a handler for an event type
	Register(eventType model.EventType, handler EventHandler)

	// RegisterDefault registers a default handler for unknown event types
	RegisterDefault(handler EventHandler)

	// Route routes an event to the appropriate handler
	Route(ctx context.Context, metadata *model.MessageMetadata, rawEvent []byte) error
}

// ConsumerInterface defines the basic methods for a NATS consumer
type ConsumerInterface interface {
	// Setup sets up the JetStream consumer
	Setup() error

	// Start sets up and starts the consumer
	Start() error

	// Stop stops the consumer
	Stop()
}

// Ensure Router implements RouterInterface
var _ RouterInterface = (*Router)(nil)

// Ensure Consumer implements ConsumerInterface
var _ ConsumerInterface = (*HistoricalConsumer)(nil)
var _ ConsumerInterface = (*RealtimeConsumer)(nil)
