package jetstream

import (
	"context"

	"github.com/nats-io/nats.go"
)

// ClientInterface defines the interface for the JetStream client
// This allows for easy mocking in tests
type ClientInterface interface {
	// SetupStream ensures the stream exists with the given configuration
	// Accepts a specific streamConfig
	SetupStream(ctx context.Context, streamConfig *nats.StreamConfig) error

	// SetupConsumer ensures the consumer exists with the given configuration for a specific stream
	// Accepts the streamName and the full consumerConfig
	SetupConsumer(ctx context.Context, streamName string, consumerConfig *nats.ConsumerConfig) error

	// Subscribe subscribes to a subject with a durable consumer
	Subscribe(subject, consumer, group string, handler nats.MsgHandler) (*nats.Subscription, error)

	// SubscribePush creates a push-based consumer subscription
	SubscribePush(subject, consumer, group, stream string, handler nats.MsgHandler) (*nats.Subscription, error)

	// SubscribePull creates a pull-based consumer subscription
	// Requires the streamName for binding
	SubscribePull(streamName, subject, consumer string) (*nats.Subscription, error)

	// Publish publishes a message to a subject with optional headers
	Publish(subject string, data []byte, headers map[string]string) error

	// Close closes the NATS connection
	Close()

	// NatsConn returns the underlying *nats.Conn
	NatsConn() *nats.Conn
}
